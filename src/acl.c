/*
 * Copyright (c) 2018-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "sha256.h"
#include <fcntl.h>
#include <ctype.h>

/* =============================================================================
 * ACL 全局状态
 * ==========================================================================*/

rax *Users; /* 将用户名映射到用户结构的表 */

user *DefaultUser;  /* 默认用户的全局引用。
                       如果没有使用 AUTH 或 HELLO 进行不同用户的认证，
                       每个新连接都会关联到此默认用户。 */

list *UsersToLoad;  /* 在配置文件中发现的用户列表，需要在 Redis 初始化
                       的最后阶段（所有模块加载完成后）进行加载。
                       每个列表元素是一个以 NULL 结尾的 SDS 指针数组：
                       第一个是用户名，其余指针是与 ACLSetUser() 相同
                       格式的 ACL 规则。 */
list *ACLLog;       /* 安全日志，用户可以使用 ACL LOG 命令查看。 */

long long ACLLogEntryCount = 0; /* 已创建的 ACL 日志条目数量 */

static rax *commandId = NULL; /* 命令名称到 ID 的映射 */

static unsigned long nextid = 0; /* 下一个尚未分配的命令 ID */

#define ACL_MAX_CATEGORIES 64 /* 命令分类的最大数量 */

struct ACLCategoryItem {
    char *name;
    uint64_t flag;
} ACLDefaultCommandCategories[] = { /* 各分类详情请参见 redis.conf。 */
    {"keyspace", ACL_CATEGORY_KEYSPACE},
    {"read", ACL_CATEGORY_READ},
    {"write", ACL_CATEGORY_WRITE},
    {"set", ACL_CATEGORY_SET},
    {"sortedset", ACL_CATEGORY_SORTEDSET},
    {"list", ACL_CATEGORY_LIST},
    {"hash", ACL_CATEGORY_HASH},
    {"string", ACL_CATEGORY_STRING},
    {"bitmap", ACL_CATEGORY_BITMAP},
    {"hyperloglog", ACL_CATEGORY_HYPERLOGLOG},
    {"geo", ACL_CATEGORY_GEO},
    {"stream", ACL_CATEGORY_STREAM},
    {"pubsub", ACL_CATEGORY_PUBSUB},
    {"admin", ACL_CATEGORY_ADMIN},
    {"fast", ACL_CATEGORY_FAST},
    {"slow", ACL_CATEGORY_SLOW},
    {"blocking", ACL_CATEGORY_BLOCKING},
    {"dangerous", ACL_CATEGORY_DANGEROUS},
    {"connection", ACL_CATEGORY_CONNECTION},
    {"transaction", ACL_CATEGORY_TRANSACTION},
    {"scripting", ACL_CATEGORY_SCRIPTING},
    {NULL,0} /* 终止符 */
};

static struct ACLCategoryItem *ACLCommandCategories = NULL;
static size_t nextCommandCategory = 0; /* 下一个待添加的命令分类索引 */

/* 实现在运行时向 ACL 分类列表添加分类的能力。由于每个 ACL 分类
 * 都需要 acl_categories 标志中的一个位，因此可添加的数量有限制。
 * 新的 ACL 分类占据 acl_categories 标志中除默认 ACL 命令分类
 * 所占用位之外的剩余位。
 *
 * 可选的 `flag` 参数允许为 ACL 分类分配 acl_categories 标志位。
 * 添加新分类时（默认 ACL 命令分类除外），此参数应为 `0`，
 * 以允许函数将下一个可用的 acl_categories 标志位分配给新分类。
 *
 * 返回 1 -> 添加成功，0 -> 失败（空间不足）
 *
 * 此函数在此处是为了访问 ACLCommandCategories 数组并添加新分类。
 */
int ACLAddCommandCategory(const char *name, uint64_t flag) {
    if (nextCommandCategory >= ACL_MAX_CATEGORIES) return 0;
    ACLCommandCategories[nextCommandCategory].name = zstrdup(name);
    ACLCommandCategories[nextCommandCategory].flag = flag != 0 ? flag : (1ULL<<nextCommandCategory);
    nextCommandCategory++;
    return 1;
}

/* 初始化 ACLCommandCategories 为默认 ACL 分类，并为新分类分配空间。 */
void ACLInitCommandCategories(void) {
    ACLCommandCategories = zcalloc(sizeof(struct ACLCategoryItem) * (ACL_MAX_CATEGORIES + 1));
    for (int j = 0; ACLDefaultCommandCategories[j].flag; j++) {
        serverAssert(ACLAddCommandCategory(ACLDefaultCommandCategories[j].name, ACLDefaultCommandCategories[j].flag));
    }
}

/* 此函数从 ACLCommandCategories 数组的末尾移除指定数量的分类。
 * 其目的是移除在 onload 函数中失败的模块所添加的分类。
 */
void ACLCleanupCategoriesOnFailure(size_t num_acl_categories_added) {
    for (size_t j = nextCommandCategory - num_acl_categories_added; j < nextCommandCategory; j++) {
        zfree(ACLCommandCategories[j].name);
        ACLCommandCategories[j].name = NULL;
        ACLCommandCategories[j].flag = 0;
    }
    nextCommandCategory -= num_acl_categories_added;
}

struct ACLUserFlag {
    const char *name;
    uint64_t flag;
} ACLUserFlags[] = {
    /* 注意：此处的顺序决定了 ACLDescribeUser 输出的顺序 */
    {"on", USER_FLAG_ENABLED},
    {"off", USER_FLAG_DISABLED},
    {"nopass", USER_FLAG_NOPASS},
    {"skip-sanitize-payload", USER_FLAG_SANITIZE_PAYLOAD_SKIP},
    {"sanitize-payload", USER_FLAG_SANITIZE_PAYLOAD},
    {NULL,0} /* 终止符 */
};

struct ACLSelectorFlags {
    const char *name;
    uint64_t flag;
} ACLSelectorFlags[] = {
    /* 注意：此处的顺序决定了 ACLDescribeUser 输出的顺序 */
    {"allkeys", SELECTOR_FLAG_ALLKEYS},
    {"allchannels", SELECTOR_FLAG_ALLCHANNELS},
    {"allcommands", SELECTOR_FLAG_ALLCOMMANDS},
    {NULL,0} /* 终止符 */
};

/* ACL 选择器是私有的，不会暴露给 acl.c 之外的代码。 */
typedef struct {
    uint32_t flags; /* 参见 SELECTOR_FLAG_* */
    /* allowed_commands 中的位被设置表示该用户有权执行此命令。
     *
     * 如果给定命令的位未设置，且该命令有允许的 first-args，
     * Redis 还会检查 allowed_firstargs 以判断命令是否可执行。 */
    uint64_t allowed_commands[USER_COMMAND_BITS_COUNT/64];
    /* allowed_firstargs 用于 ACL 规则，除非提供特定的 argv[1]，
     * 否则阻止对命令的访问。
     *
     * 对于每个命令 ID（对应于 allowed_commands 中设置的命令位），
     * 此数组指向一个 SDS 字符串数组（以 NULL 指针终止），
     * 包含该命令所有允许的 first-args。当不使用 first-arg 匹配时，
     * 该字段设置为 NULL 以避免分配 USER_COMMAND_BITS_COUNT 个指针。 */
    sds **allowed_firstargs;
    list *patterns;  /* 允许的键模式列表。如果此字段为 NULL，
                        用户不能在命令中提及任何键，
                        除非设置了 ALLKEYS 标志。 */
    list *channels;  /* 允许的 Pub/Sub 频道模式列表。如果此字段为 NULL，
                        用户不能在 PUBLISH 或 [P][UNSUBSCRIBE] 命令中
                        提及任何频道，除非设置了 ALLCHANNELS 标志。 */
    sds command_rules; /* 有序分类和命令的字符串表示，用于重新生成
                        * 原始 ACL 字符串以供显示。 */
} aclSelector;

void ACLResetFirstArgsForCommand(aclSelector *selector, unsigned long id);
void ACLResetFirstArgs(aclSelector *selector);
void ACLAddAllowedFirstArg(aclSelector *selector, unsigned long id, const char *sub);
void ACLFreeLogEntry(void *le);
int ACLSetSelector(aclSelector *selector, const char *op, size_t oplen);

/* 哈希密码的字符串表示长度 */
#define HASH_PASSWORD_LEN (SHA256_BLOCK_SIZE*2)

/* =============================================================================
 * ACL 实现的辅助函数
 * ==========================================================================*/

/* 如果字符串相同返回零，不同返回非零。
 * 比较方式可防止攻击者仅通过监控函数执行时间
 * 来获取字符串性质的信息。注意：两个字符串必须长度相同。
 */
int time_independent_strcmp(char *a, char *b, int len) {
    int diff = 0;
    for (int j = 0; j < len; j++) {
        diff |= (a[j] ^ b[j]);
    }
    return diff; /* 为零表示字符串相同 */
}

/* 给定一个 SDS 字符串，返回其 SHA256 十六进制表示的新 SDS 字符串。 */
sds ACLHashPassword(unsigned char *cleartext, size_t len) {
    SHA256_CTX ctx;
    unsigned char hash[SHA256_BLOCK_SIZE];
    char hex[HASH_PASSWORD_LEN];
    char *cset = "0123456789abcdef";

    sha256_init(&ctx);
    sha256_update(&ctx,(unsigned char*)cleartext,len);
    sha256_final(&ctx,hash);

    for (int j = 0; j < SHA256_BLOCK_SIZE; j++) {
        hex[j*2] = cset[((hash[j]&0xF0)>>4)];
        hex[j*2+1] = cset[(hash[j]&0xF)];
    }
    return sdsnewlen(hex,HASH_PASSWORD_LEN);
}

/* 给定哈希值和哈希长度，如果是有效的密码哈希返回 C_OK，否则返回 C_ERR。 */
int ACLCheckPasswordHash(unsigned char *hash, int hashlen) {
    if (hashlen != HASH_PASSWORD_LEN) {
        return C_ERR;
    }

    /* 密码哈希只能包含表示十六进制值的字符，
     * 即数字和小写字母 'a' 到 'f'。 */
    for(int i = 0; i < HASH_PASSWORD_LEN; i++) {
        char c = hash[i];
        if ((c < 'a' || c > 'f') && (c < '0' || c > '9')) {
            return C_ERR;
        }
    }
    return C_OK;
}

/* =============================================================================
 * 底层 ACL API
 * ==========================================================================*/

/* 如果指定字符串包含空格或空字符返回 1。
 * 对用户名和键模式进行此检查是为了简化 ACL 规则的重写、
 * ACL 列表的展示，以及避免在存在转义时解析规则可能产生的
 * 细微安全漏洞。如果字符串没有空格返回 0。 */
int ACLStringHasSpaces(const char *s, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (isspace(s[i]) || s[i] == 0) return 1;
    }
    return 0;
}

/* 根据分类名称返回对应的标志，如果没有匹配则返回零。 */
uint64_t ACLGetCommandCategoryFlagByName(const char *name) {
    for (int j = 0; ACLCommandCategories[j].flag != 0; j++) {
        if (!strcasecmp(name,ACLCommandCategories[j].name)) {
            return ACLCommandCategories[j].flag;
        }
    }
    return 0; /* 无匹配 */
}

/* 在用户定义列表中搜索用户的方法。列表包含用户参数数组，
 * 我们只搜索第一个参数（用户名）进行匹配。 */
int ACLListMatchLoadedUser(void *definition, void *user) {
    sds *user_definition = definition;
    return sdscmp(user_definition[0], user) == 0;
}

/* 用于 user->passwords 列表的密码/模式比较方法，
 * 以便使用 listSearchKey() 搜索条目。 */
int ACLListMatchSds(void *a, void *b) {
    return sdscmp(a,b) == 0;
}

/* 释放 ACL 用户密码/模式列表元素的方法 */
void ACLListFreeSds(void *item) {
    sdsfree(item);
}

/* 复制 ACL 用户密码/模式列表元素的方法 */
void *ACLListDupSds(void *item) {
    return sdsdup(item);
}

/* 用于处理具有不同键权限的键模式的结构体 */
typedef struct {
    int flags; /* 此键模式的 ACL 键权限类型 */
    sds pattern; /* 用于匹配键的模式 */
} keyPattern;

/* 创建一个新的键模式 */
keyPattern *ACLKeyPatternCreate(sds pattern, int flags) {
    keyPattern *new = (keyPattern *) zmalloc(sizeof(keyPattern));
    new->pattern = pattern;
    new->flags = flags;
    return new;
}

/* 释放键模式及其内部结构 */
void ACLKeyPatternFree(keyPattern *pattern) {
    sdsfree(pattern->pattern);
    zfree(pattern);
}

/* 用于 user->passwords 列表的键模式比较方法，
 * 以便使用 listSearchKey() 搜索条目。 */
int ACLListMatchKeyPattern(void *a, void *b) {
    return sdscmp(((keyPattern *) a)->pattern,((keyPattern *) b)->pattern) == 0;
}

/* 释放 ACL 用户键模式列表元素的方法 */
void ACLListFreeKeyPattern(void *item) {
    ACLKeyPatternFree(item);
}

/* 复制 ACL 用户键模式列表元素的方法 */
void *ACLListDupKeyPattern(void *item) {
    keyPattern *old = (keyPattern *) item;
    return ACLKeyPatternCreate(sdsdup(old->pattern), old->flags);
}

/* 将键模式的字符串表示追加到提供的基础字符串上。 */
sds sdsCatPatternString(sds base, keyPattern *pat) {
    if (pat->flags == ACL_ALL_PERMISSION) {
        base = sdscatlen(base,"~",1);
    } else if (pat->flags == ACL_READ_PERMISSION) {
        base = sdscatlen(base,"%R~",3);
    } else if (pat->flags == ACL_WRITE_PERMISSION) {
        base = sdscatlen(base,"%W~",3);
    } else {
        serverPanic("Invalid key pattern flag detected");
    }
    return sdscatsds(base, pat->pattern);
}

/* 创建一个具有提供的初始标志集的空选择器。
 * 选择器默认没有任何权限。 */
aclSelector *ACLCreateSelector(int flags) {
    aclSelector *selector = zmalloc(sizeof(aclSelector));
    selector->flags = flags | server.acl_pubsub_default;
    selector->patterns = listCreate();
    selector->channels = listCreate();
    selector->allowed_firstargs = NULL;
    selector->command_rules = sdsempty();

    listSetMatchMethod(selector->patterns,ACLListMatchKeyPattern);
    listSetFreeMethod(selector->patterns,ACLListFreeKeyPattern);
    listSetDupMethod(selector->patterns,ACLListDupKeyPattern);
    listSetMatchMethod(selector->channels,ACLListMatchSds);
    listSetFreeMethod(selector->channels,ACLListFreeSds);
    listSetDupMethod(selector->channels,ACLListDupSds);
    memset(selector->allowed_commands,0,sizeof(selector->allowed_commands));

    return selector;
}

/* 清理提供的选择器，包括所有内部结构。 */
void ACLFreeSelector(aclSelector *selector) {
    listRelease(selector->patterns);
    listRelease(selector->channels);
    sdsfree(selector->command_rules);
    ACLResetFirstArgs(selector);
    zfree(selector);
}

/* 创建提供的选择器的精确副本。 */
aclSelector *ACLCopySelector(aclSelector *src) {
    aclSelector *dst = zmalloc(sizeof(aclSelector));
    dst->flags = src->flags;
    dst->patterns = listDup(src->patterns);
    dst->channels = listDup(src->channels);
    dst->command_rules = sdsdup(src->command_rules);
    memcpy(dst->allowed_commands,src->allowed_commands,
           sizeof(dst->allowed_commands));
    dst->allowed_firstargs = NULL;
    /* 复制允许的 first-args SDS 字符串数组的数组。 */
    if (src->allowed_firstargs) {
        for (int j = 0; j < USER_COMMAND_BITS_COUNT; j++) {
            if (!(src->allowed_firstargs[j])) continue;
            for (int i = 0; src->allowed_firstargs[j][i]; i++) {
                ACLAddAllowedFirstArg(dst, j, src->allowed_firstargs[j][i]);
            }
        }
    }
    return dst;
}

/* 释放选择器的列表方法 */
void ACLListFreeSelector(void *a) {
    ACLFreeSelector((aclSelector *) a);
}

/* 复制选择器的列表方法 */
void *ACLListDuplicateSelector(void *src) {
    return ACLCopySelector((aclSelector *)src);
}

/* 所有用户都有一个隐式的根选择器，
 * 为旧的 ACL 权限提供向后兼容。 */
aclSelector *ACLUserGetRootSelector(user *u) {
    serverAssert(listLength(u->selectors));
    aclSelector *s = (aclSelector *) listNodeValue(listFirst(u->selectors));
    serverAssert(s->flags & SELECTOR_FLAG_ROOT);
    return s;
}

/* 创建一个具有指定名称的新用户，将其存储在用户列表
 * （Users 全局基数树）中，并返回该用户结构的引用。
 *
 * 如果该名称的用户已存在则返回 NULL。 */
user *ACLCreateUser(const char *name, size_t namelen) {
    if (raxFind(Users,(unsigned char*)name,namelen,NULL)) return NULL;
    user *u = zmalloc(sizeof(*u));
    u->name = sdsnewlen(name,namelen);
    u->flags = USER_FLAG_DISABLED;
    u->flags |= USER_FLAG_SANITIZE_PAYLOAD;
    u->passwords = listCreate();
    u->acl_string = NULL;
    listSetMatchMethod(u->passwords,ACLListMatchSds);
    listSetFreeMethod(u->passwords,ACLListFreeSds);
    listSetDupMethod(u->passwords,ACLListDupSds);

    u->selectors = listCreate();
    listSetFreeMethod(u->selectors,ACLListFreeSelector);
    listSetDupMethod(u->selectors,ACLListDuplicateSelector);

    /* 添加初始根选择器 */
    aclSelector *s = ACLCreateSelector(SELECTOR_FLAG_ROOT);
    listAddNodeHead(u->selectors, s);

    raxInsert(Users,(unsigned char*)name,namelen,u,NULL);
    return u;
}

/* 当我们需要一个未关联的"假"用户来验证 ACL 规则或类似原因时，
 * 应调用此函数。该用户不会链接到 Users 基数树。
 * 返回的用户应像往常一样使用 ACLFreeUser() 释放。 */
user *ACLCreateUnlinkedUser(void) {
    char username[64];
    for (int j = 0; ; j++) {
        snprintf(username,sizeof(username),"__fakeuser:%d__",j);
        user *fakeuser = ACLCreateUser(username,strlen(username));
        if (fakeuser == NULL) continue;
        int retval = raxRemove(Users,(unsigned char*) username,
                               strlen(username),NULL);
        serverAssert(retval != 0);
        return fakeuser;
    }
}

/* 释放用户结构占用的内存。注意此函数不会从
 * Users 全局基数树中移除该用户。 */
void ACLFreeUser(user *u) {
    sdsfree(u->name);
    if (u->acl_string) {
        decrRefCount(u->acl_string);
        u->acl_string = NULL;
    }
    listRelease(u->passwords);
    listRelease(u->selectors);
    zfree(u);
}

/* 删除用户时需要遍历活动连接，以终止所有已使用
 * 该用户认证的待处理连接。 */
void ACLFreeUserAndKillClients(user *u) {
    listIter li;
    listNode *ln;
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (c->user == u) {
            /* 我们将异步释放连接，因此理论上不需要设置不同的用户。
             * 但如果 Redis 中存在 bug，这迟早可能导致安全漏洞：
             * 设置默认用户并将其置于未认证模式更具防御性。 */
            deauthenticateAndCloseClient(c);
        }
    }
    ACLFreeUser(u);
}

/* 将源用户 'src' 的 ACL 规则复制到目标用户 'dst'，
 * 使得处理完成后两者拥有完全相同的规则（但名称保持原样）。 */
void ACLCopyUser(user *dst, user *src) {
    listRelease(dst->passwords);
    listRelease(dst->selectors);
    dst->passwords = listDup(src->passwords);
    dst->selectors = listDup(src->selectors);
    dst->flags = src->flags;
    if (dst->acl_string) {
        decrRefCount(dst->acl_string);
    }
    dst->acl_string = src->acl_string;
    if (dst->acl_string) {
        /* 如果 src 为 NULL，我们设为 NULL；否则需要增加引用计数 */
        incrRefCount(dst->acl_string);
    }
}

/* 给定命令 ID，此函数通过引用设置 'word' 和 'bit'，
 * 使得 user->allowed_commands[word] 定位到存储该 ID 对应位的正确字，
 * user->allowed_commands[word]&bit 标识该特定位。
 * 如果指定 ID 超出用户位图表示范围，返回 C_ERR。 */
int ACLGetCommandBitCoordinates(uint64_t id, uint64_t *word, uint64_t *bit) {
    if (id >= USER_COMMAND_BITS_COUNT) return C_ERR;
    *word = id / sizeof(uint64_t) / 8;
    *bit = 1ULL << (id % (sizeof(uint64_t) * 8));
    return C_OK;
}

/* 检查指定用户是否设置了指定的命令位。
 * 如果位已设置返回 1，否则返回 0。
 * 注意此函数不检查用户的 ALLCOMMANDS 标志，
 * 只检查底层位掩码。
 *
 * 如果位超出用户内部表示范围，返回零以禁止在此边界情况下执行命令。 */
int ACLGetSelectorCommandBit(const aclSelector *selector, unsigned long id) {
    uint64_t word, bit;
    if (ACLGetCommandBitCoordinates(id,&word,&bit) == C_ERR) return 0;
    return (selector->allowed_commands[word] & bit) != 0;
}

/* 当给出 +@all 或 allcommands 时，我们同时设置一个保留位，
 * 稍后可以测试该位来判断用户是否有权执行"未来命令"，
 * 即之后通过模块加载的命令。 */
int ACLSelectorCanExecuteFutureCommands(aclSelector *selector) {
    return ACLGetSelectorCommandBit(selector,USER_COMMAND_BITS_COUNT-1);
}

/* 将指定用户的指定命令位设置为 'value'（0 或 1）。
 * 如果位超出用户内部表示范围，不执行任何操作。
 * 以零值调用此函数的副作用是清除用户 ALLCOMMANDS 标志，
 * 因为不再可能跳过命令位的显式测试。 */
void ACLSetSelectorCommandBit(aclSelector *selector, unsigned long id, int value) {
    uint64_t word, bit;
    if (ACLGetCommandBitCoordinates(id,&word,&bit) == C_ERR) return;
    if (value) {
        selector->allowed_commands[word] |= bit;
    } else {
        selector->allowed_commands[word] &= ~bit;
        selector->flags &= ~SELECTOR_FLAG_ALLCOMMANDS;
    }
}

/* 从保留的命令规则中移除一条规则。始终逐字匹配规则，
 * 但如果正在添加或移除整个命令，也会移除子命令规则。 */
void ACLSelectorRemoveCommandRule(aclSelector *selector, sds new_rule) {
    size_t new_len = sdslen(new_rule);
    char *existing_rule = selector->command_rules;

    /* 遍历现有规则，尝试找到与新规则"匹配"的规则。
     * 如果找到匹配项，则通过将后续规则复制过来以从字符串中移除该命令。 */
    while(existing_rule[0]) {
        /* 规则的第一个字符是 +/-，不需要比较。 */
        char *copy_position = existing_rule;
        existing_rule += 1;

        /* 假设命令后的尾随空格是命令的一部分，如 '+get '，
         * 因此移除命令时也一并修剪。 */
        char *rule_end = strchr(existing_rule, ' ');
        if (!rule_end) {
            /* 这是最后一条规则，将其移到字符串末尾。 */
            rule_end = existing_rule + strlen(existing_rule);

            /* 如果移除最后一条规则，此方法可能留下尾随空格，
             * 但仅当它不是第一条规则时，因此需要处理这种情况。 */
            if (copy_position != selector->command_rules) copy_position -= 1;
        }
        char *copy_end = rule_end;
        if (*copy_end == ' ') copy_end++;

        /* 精确匹配或正在比较的规则是由 '|' 表示的子命令 */
        size_t existing_len = rule_end - existing_rule;
        if (!memcmp(existing_rule, new_rule, min(existing_len, new_len))) {
            if ((existing_len == new_len) || (existing_len > new_len && (existing_rule[new_len]) == '|')) {
                /* 从下一条规则开始复制剩余规则以替换要删除的规则，
                 * 包括终止 NULL 字符。 */
                memmove(copy_position, copy_end, strlen(copy_end) + 1);
                existing_rule = copy_position;
                continue;
            }
        }
        existing_rule = copy_end;
    }

    /* 规则末尾现在有多余的填充，清理它。 */
    sdsupdatelen(selector->command_rules);
}

/* 此函数负责更新 command_rules 结构，以维护命令和分类的相对顺序，
 * 并能无损地重现。 */
void ACLUpdateCommandRules(aclSelector *selector, const char *rule, int allow) {
    sds new_rule = sdsnew(rule);
    sdstolower(new_rule);

    ACLSelectorRemoveCommandRule(selector, new_rule);
    if (sdslen(selector->command_rules)) selector->command_rules = sdscat(selector->command_rules, " ");
    selector->command_rules = sdscatfmt(selector->command_rules, allow ? "+%S" : "-%S", new_rule);
    sdsfree(new_rule);
}

/* 此函数用于允许/阻止特定命令。
 * 允许/阻止容器命令也会应用于其子命令。 */
void ACLChangeSelectorPerm(aclSelector *selector, struct redisCommand *cmd, int allow) {
    unsigned long id = cmd->id;
    ACLSetSelectorCommandBit(selector,id,allow);
    ACLResetFirstArgsForCommand(selector,id);
    if (cmd->subcommands_dict) {
        dictEntry *de;
        dictIterator *di = dictGetSafeIterator(cmd->subcommands_dict);
        while((de = dictNext(di)) != NULL) {
            struct redisCommand *sub = (struct redisCommand *)dictGetVal(de);
            ACLSetSelectorCommandBit(selector,sub->id,allow);
        }
        dictReleaseIterator(di);
    }
}

/* 类似于 ACLSetSelectorCommandBit()，但不是设置指定的 ID，
 * 而是检查作为参数指定的分类中的所有命令，
 * 并将这些命令对应的所有位设置为指定的值。
 * 由于用户传递的分类可能不存在，如果分类未找到返回 C_ERR，
 * 如果找到并执行了操作返回 C_OK。 */
void ACLSetSelectorCommandBitsForCategory(dict *commands, aclSelector *selector, uint64_t cflag, int value) {
    dictIterator *di = dictGetIterator(commands);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        struct redisCommand *cmd = dictGetVal(de);
        if (cmd->acl_categories & cflag) {
            ACLChangeSelectorPerm(selector,cmd,value);
        }
        if (cmd->subcommands_dict) {
            ACLSetSelectorCommandBitsForCategory(cmd->subcommands_dict, selector, cflag, value);
        }
    }
    dictReleaseIterator(di);
}

/* 此函数负责重新计算所有现有用户的选择器的命令位。
 * 它使用 'command_rules'（有序分类和命令的字符串表示）
 * 来重新计算命令位。 */
void ACLRecomputeCommandBitsFromCommandRulesAllUsers(void) {
    raxIterator ri;
    raxStart(&ri,Users);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        user *u = ri.data;
        listIter li;
        listNode *ln;
        listRewind(u->selectors,&li);
        while((ln = listNext(&li))) {
            aclSelector *selector = (aclSelector *) listNodeValue(ln);
            int argc = 0;
            sds *argv = sdssplitargs(selector->command_rules, &argc);
            serverAssert(argv != NULL);
            /* 检查选择器对所有命令的权限，以从干净状态开始。 */
            if (ACLSelectorCanExecuteFutureCommands(selector)) {
                int res = ACLSetSelector(selector,"+@all",-1);
                serverAssert(res == C_OK);
            } else {
                int res = ACLSetSelector(selector,"-@all",-1);
                serverAssert(res == C_OK);
            }

            /* 将所有命令和分类应用到此选择器。 */
            for(int i = 0; i < argc; i++) {
                int res = ACLSetSelector(selector, argv[i], sdslen(argv[i]));
                serverAssert(res == C_OK);
            }
            sdsfreesplitres(argv, argc);
        }
    }
    raxStop(&ri);

}

int ACLSetSelectorCategory(aclSelector *selector, const char *category, int allow) {
    uint64_t cflag = ACLGetCommandCategoryFlagByName(category + 1);
    if (!cflag) return C_ERR;

    ACLUpdateCommandRules(selector, category, allow);

    /* 在选择器上设置实际的命令位。 */
    ACLSetSelectorCommandBitsForCategory(server.orig_commands, selector, cflag, allow);
    return C_OK;
}

void ACLCountCategoryBitsForCommands(dict *commands, aclSelector *selector, unsigned long *on, unsigned long *off, uint64_t cflag) {
    dictIterator *di = dictGetIterator(commands);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        struct redisCommand *cmd = dictGetVal(de);
        if (cmd->acl_categories & cflag) {
            if (ACLGetSelectorCommandBit(selector,cmd->id))
                (*on)++;
            else
                (*off)++;
        }
        if (cmd->subcommands_dict) {
            ACLCountCategoryBitsForCommands(cmd->subcommands_dict, selector, on, off, cflag);
        }
    }
    dictReleaseIterator(di);
}

/* 返回用户 'u' 在指定分类名称标记的命令子集中
 * 允许（on）和拒绝（off）的命令数量。
 * 如果分类名称无效返回 C_ERR，否则返回 C_OK
 * 并通过引用填充 on 和 off。 */
int ACLCountCategoryBitsForSelector(aclSelector *selector, unsigned long *on, unsigned long *off,
                                const char *category)
{
    uint64_t cflag = ACLGetCommandCategoryFlagByName(category);
    if (!cflag) return C_ERR;

    *on = *off = 0;
    ACLCountCategoryBitsForCommands(server.orig_commands, selector, on, off, cflag);
    return C_OK;
}

/* 此函数返回一个 SDS 字符串，表示指定选择器与命令执行相关的
 * ACL 规则，格式与 ACL SETUSER 设置时相同。函数只返回重新创建
 * 用户命令位图所需的规则集，不包括其他用户标志如 on/off、密码等。
 * 返回的字符串始终以 +@all 或 -@all 规则开头（取决于用户位图），
 * 如有必要后跟缩小或扩展用户权限的其他规则。 */
sds ACLDescribeSelectorCommandRules(aclSelector *selector) {
    sds rules = sdsempty();

    /* 使用此假选择器作为"健全性"检查，确保生成的规则
     * 与当前选择器具有相同的位图。 */
    aclSelector *fake_selector = ACLCreateSelector(0);

    /* 此处需要判断应以 +@all 还是 -@all 开头。
     * 注意以 +@all 开头并做减法时，用户可以执行未来的命令；
     * 而以 -@all 开头并做加法时，只允许用户运行选定的命令和/或分类。
     * 如何测试？我们使用一个保留命令 ID 位的技巧，
     * 该位仅由 +@all（及其别名 "allcommands"）设置。 */
    if (ACLSelectorCanExecuteFutureCommands(selector)) {
        rules = sdscat(rules,"+@all ");
        ACLSetSelector(fake_selector,"+@all",-1);
    } else {
        rules = sdscat(rules,"-@all ");
        ACLSetSelector(fake_selector,"-@all",-1);
    }

    /* 将所有命令和分类应用到假选择器。 */
    int argc = 0;
    sds *argv = sdssplitargs(selector->command_rules, &argc);
    serverAssert(argv != NULL);

    for(int i = 0; i < argc; i++) {
        int res = ACLSetSelector(fake_selector, argv[i], -1);
        serverAssert(res == C_OK);
    }
    if (sdslen(selector->command_rules)) {
        rules = sdscatfmt(rules, "%S ", selector->command_rules);
    }
    sdsfreesplitres(argv, argc);

    /* 修剪最后无用的空格。 */
    sdsrange(rules,0,-2);

    /* 这在技术上不是必需的，但我们希望验证预测的位图
     * 与用户位图完全相同，否则中止，因为在此代码路径中
     * 中止比安全风险更好。 */
    if (memcmp(fake_selector->allowed_commands,
                        selector->allowed_commands,
                        sizeof(selector->allowed_commands)) != 0)
    {
        serverLog(LL_WARNING,
            "CRITICAL ERROR: User ACLs don't match final bitmap: '%s'",
            rules);
        serverPanic("No bitmap match in ACLDescribeSelectorCommandRules()");
    }
    ACLFreeSelector(fake_selector);
    return rules;
}

sds ACLDescribeSelector(aclSelector *selector) {
    listIter li;
    listNode *ln;
    sds res = sdsempty();
    /* 键模式 */
    if (selector->flags & SELECTOR_FLAG_ALLKEYS) {
        res = sdscatlen(res,"~* ",3);
    } else {
        listRewind(selector->patterns,&li);
        while((ln = listNext(&li))) {
            keyPattern *thispat = (keyPattern *)listNodeValue(ln);
            res = sdsCatPatternString(res, thispat);
            res = sdscatlen(res," ",1);
        }
    }

    /* Pub/Sub 频道模式 */
    if (selector->flags & SELECTOR_FLAG_ALLCHANNELS) {
        res = sdscatlen(res,"&* ",3);
    } else {
        res = sdscatlen(res,"resetchannels ",14);
        listRewind(selector->channels,&li);
        while((ln = listNext(&li))) {
            sds thispat = listNodeValue(ln);
            res = sdscatlen(res,"&",1);
            res = sdscatsds(res,thispat);
            res = sdscatlen(res," ",1);
        }
    }

    /* 命令规则 */
    sds rules = ACLDescribeSelectorCommandRules(selector);
    res = sdscatsds(res,rules);
    sdsfree(rules);
    return res;
}

/* 这类似于 ACLDescribeSelectorCommandRules()，但不是只描述用户命令规则，
 * 而是描述所有内容：用户标志、键、密码以及通过 ACLDescribeSelectorCommandRules()
 * 函数获得的命令规则。当我们想要重写描述 ACL 的配置文件
 * 以及使用 ACL LIST 显示用户时调用此函数。 */
robj *ACLDescribeUser(user *u) {
    if (u->acl_string) {
        incrRefCount(u->acl_string);
        return u->acl_string;
    }

    sds res = sdsempty();

    /* 标志 */
    for (int j = 0; ACLUserFlags[j].flag; j++) {
        if (u->flags & ACLUserFlags[j].flag) {
            res = sdscat(res,ACLUserFlags[j].name);
            res = sdscatlen(res," ",1);
        }
    }

    /* 密码 */
    listIter li;
    listNode *ln;
    listRewind(u->passwords,&li);
    while((ln = listNext(&li))) {
        sds thispass = listNodeValue(ln);
        res = sdscatlen(res,"#",1);
        res = sdscatsds(res,thispass);
        res = sdscatlen(res," ",1);
    }

    /* 选择器（命令和键） */
    listRewind(u->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *selector = (aclSelector *) listNodeValue(ln);
        sds default_perm = ACLDescribeSelector(selector);
        if (selector->flags & SELECTOR_FLAG_ROOT) {
            res = sdscatfmt(res, "%s", default_perm);
        } else {
            res = sdscatfmt(res, " (%s)", default_perm);
        }
        sdsfree(default_perm);
    }

    u->acl_string = createObject(OBJ_STRING, res);
    /* 因为要返回它，所以需要增加引用计数 */
    incrRefCount(u->acl_string);

    return u->acl_string;
}

/* 从原始命令表中获取命令，不受命令重命名操作的影响：
 * 我们基于该表进行所有 ACL 工作，因此无论命令如何重命名 ACL 都有效。 */
struct redisCommand *ACLLookupCommand(const char *name) {
    struct redisCommand *cmd;
    sds sdsname = sdsnew(name);
    cmd = lookupCommandBySdsLogic(server.orig_commands,sdsname);
    sdsfree(sdsname);
    return cmd;
}

/* 清空指定用户和命令 ID 的允许 first-args 数组。 */
void ACLResetFirstArgsForCommand(aclSelector *selector, unsigned long id) {
    if (selector->allowed_firstargs && selector->allowed_firstargs[id]) {
        for (int i = 0; selector->allowed_firstargs[id][i]; i++)
            sdsfree(selector->allowed_firstargs[id][i]);
        zfree(selector->allowed_firstargs[id]);
        selector->allowed_firstargs[id] = NULL;
    }
}

/* 清空整个 first-args 表。这在 +@all、-@all 或类似操作时有用，
 * 可使用户回到最小内存使用（和最少检查）状态。 */
void ACLResetFirstArgs(aclSelector *selector) {
    if (selector->allowed_firstargs == NULL) return;
    for (int j = 0; j < USER_COMMAND_BITS_COUNT; j++) {
        if (selector->allowed_firstargs[j]) {
            for (int i = 0; selector->allowed_firstargs[j][i]; i++)
                sdsfree(selector->allowed_firstargs[j][i]);
            zfree(selector->allowed_firstargs[j]);
        }
    }
    zfree(selector->allowed_firstargs);
    selector->allowed_firstargs = NULL;
}

/* 为用户 'u' 和指定的命令 ID 向子命令列表添加一个 first-arg。 */
void ACLAddAllowedFirstArg(aclSelector *selector, unsigned long id, const char *sub) {
    /* 如果这是此用户配置的第一个 first-arg，
     * 我们需要分配 first-args 数组。 */
    if (selector->allowed_firstargs == NULL) {
        selector->allowed_firstargs = zcalloc(USER_COMMAND_BITS_COUNT * sizeof(sds*));
    }

    /* 我们还需要扩大指向以 NULL 终止的 SDS 数组的分配，
     * 为此项腾出空间。首先检查当前大小，
     * 同时确保该 first-arg 尚未在其中指定。 */
    long items = 0;
    if (selector->allowed_firstargs[id]) {
        while(selector->allowed_firstargs[id][items]) {
            /* 如果已存在则不再添加 */
            if (!strcasecmp(selector->allowed_firstargs[id][items],sub))
                return;
            items++;
        }
    }

    /* 现在为新项（和 NULL 终止符）腾出空间。 */
    items += 2;
    selector->allowed_firstargs[id] = zrealloc(selector->allowed_firstargs[id], sizeof(sds)*items);
    selector->allowed_firstargs[id][items-2] = sdsnew(sub);
    selector->allowed_firstargs[id][items-1] = NULL;
}

/* 从给定的 ACL 操作创建 ACL 选择器，操作应为以括号开头和结尾的
 * 空格分隔的 ACL 操作列表。
 *
 * 如果任何操作无效，将返回 NULL，
 * errno 将设置为对应的内部错误。 */
aclSelector *aclCreateSelectorFromOpSet(const char *opset, size_t opsetlen) {
    serverAssert(opset[0] == '(' && opset[opsetlen - 1] == ')');
    aclSelector *s = ACLCreateSelector(0);

    int argc = 0;
    sds trimmed = sdsnewlen(opset + 1, opsetlen - 2);
    sds *argv = sdssplitargs(trimmed, &argc);
    for (int i = 0; i < argc; i++) {
        if (ACLSetSelector(s, argv[i], sdslen(argv[i])) == C_ERR) {
            ACLFreeSelector(s);
            s = NULL;
            goto cleanup;
        }
    }

cleanup:
    sdsfreesplitres(argv, argc);
    sdsfree(trimmed);
    return s;
}

/* 使用提供的 'op' 设置选择器的属性。
 *
 * +<command>   允许执行该命令。
 *              可与 `|` 配合使用以允许子命令（如 "+config|get"）
 * -<command>   禁止执行该命令。
 *              可与 `|` 配合使用以阻止子命令（如 "-config|set"）
 * +@<category> 允许执行该分类中的所有命令，
 *              有效分类如 @admin、@set、@sortedset 等，
 *              完整列表见 server.c 文件中 Redis 命令表的定义。
 *              特殊分类 @all 表示所有命令，包括当前已有的
 *              和未来通过模块加载的。
 * +<command>|first-arg    允许一个原本被禁用命令的特定第一个参数。
 *                         注意此形式不允许作为负向操作如 -SELECT|1，
 *                         只能以 "+" 开头进行正向添加。
 * allcommands  +@all 的别名。注意它意味着能够执行通过模块系统
 *              加载的所有未来命令。
 * nocommands   -@all 的别名。
 * ~<pattern>   添加可在命令中提及的键模式。例如 ~* 允许所有键。
 *              该模式是 glob 风格的模式，类似于 KEYS 使用的模式。
 *              可以指定多个模式。
 * %R~<pattern> 添加键读取模式，指定可从中读取的键。
 * %W~<pattern> 添加键写入模式，指定可写入的键。
 * allkeys      ~* 的别名
 * resetkeys    清空允许的键模式列表。
 * &<pattern>   添加可在 Pub/Sub 命令中提及的频道模式。
 *              例如 &* 允许所有频道。该模式是 glob 风格的模式，
 *              类似于 PSUBSCRIBE 使用的模式。可以指定多个模式。
 * allchannels              &* 的别名
 * resetchannels            清空允许的频道模式列表。
 */
int ACLSetSelector(aclSelector *selector, const char* op, size_t oplen) {
    if (!strcasecmp(op,"allkeys") ||
               !strcasecmp(op,"~*"))
    {
        selector->flags |= SELECTOR_FLAG_ALLKEYS;
        listEmpty(selector->patterns);
    } else if (!strcasecmp(op,"resetkeys")) {
        selector->flags &= ~SELECTOR_FLAG_ALLKEYS;
        listEmpty(selector->patterns);
    } else if (!strcasecmp(op,"allchannels") ||
               !strcasecmp(op,"&*"))
    {
        selector->flags |= SELECTOR_FLAG_ALLCHANNELS;
        listEmpty(selector->channels);
    } else if (!strcasecmp(op,"resetchannels")) {
        selector->flags &= ~SELECTOR_FLAG_ALLCHANNELS;
        listEmpty(selector->channels);
    } else if (!strcasecmp(op,"allcommands") ||
               !strcasecmp(op,"+@all"))
    {
        memset(selector->allowed_commands,255,sizeof(selector->allowed_commands));
        selector->flags |= SELECTOR_FLAG_ALLCOMMANDS;
        sdsclear(selector->command_rules);
        ACLResetFirstArgs(selector);
    } else if (!strcasecmp(op,"nocommands") ||
               !strcasecmp(op,"-@all"))
    {
        memset(selector->allowed_commands,0,sizeof(selector->allowed_commands));
        selector->flags &= ~SELECTOR_FLAG_ALLCOMMANDS;
        sdsclear(selector->command_rules);
        ACLResetFirstArgs(selector);
    } else if (op[0] == '~' || op[0] == '%') {
        if (selector->flags & SELECTOR_FLAG_ALLKEYS) {
            errno = EEXIST;
            return C_ERR;
        }
        int flags = 0;
        size_t offset = 1;
        if (op[0] == '%') {
            int perm_ok = 1;
            for (; offset < oplen; offset++) {
                if (toupper(op[offset]) == 'R' && !(flags & ACL_READ_PERMISSION)) {
                    flags |= ACL_READ_PERMISSION;
                } else if (toupper(op[offset]) == 'W' && !(flags & ACL_WRITE_PERMISSION)) {
                    flags |= ACL_WRITE_PERMISSION;
                } else if (op[offset] == '~') {
                    offset++;
                    break;
                } else {
                    perm_ok = 0;
                    break;
                }
            }
            if (!flags || !perm_ok) {
                errno = EINVAL;
                return C_ERR;
            }
        } else {
            flags = ACL_ALL_PERMISSION;
        }

        if (ACLStringHasSpaces(op+offset,oplen-offset)) {
            errno = EINVAL;
            return C_ERR;
        }
        keyPattern *newpat = ACLKeyPatternCreate(sdsnewlen(op+offset,oplen-offset), flags);
        listNode *ln = listSearchKey(selector->patterns,newpat);
        /* 避免多次添加相同的键模式。 */
        if (ln == NULL) {
            listAddNodeTail(selector->patterns,newpat);
        } else {
            ((keyPattern *)listNodeValue(ln))->flags |= flags;
            ACLKeyPatternFree(newpat);
        }
        selector->flags &= ~SELECTOR_FLAG_ALLKEYS;
    } else if (op[0] == '&') {
        if (selector->flags & SELECTOR_FLAG_ALLCHANNELS) {
            errno = EISDIR;
            return C_ERR;
        }
        if (ACLStringHasSpaces(op+1,oplen-1)) {
            errno = EINVAL;
            return C_ERR;
        }
        sds newpat = sdsnewlen(op+1,oplen-1);
        listNode *ln = listSearchKey(selector->channels,newpat);
        /* 避免多次添加相同的频道模式。 */
        if (ln == NULL)
            listAddNodeTail(selector->channels,newpat);
        else
            sdsfree(newpat);
        selector->flags &= ~SELECTOR_FLAG_ALLCHANNELS;
    } else if (op[0] == '+' && op[1] != '@') {
        if (strrchr(op,'|') == NULL) {
            struct redisCommand *cmd = ACLLookupCommand(op+1);
            if (cmd == NULL) {
                errno = ENOENT;
                return C_ERR;
            }
            ACLChangeSelectorPerm(selector,cmd,1);
            ACLUpdateCommandRules(selector,cmd->fullname,1);
        } else {
            /* 分割命令和子命令部分。 */
            char *copy = zstrdup(op+1);
            char *sub = strrchr(copy,'|');
            sub[0] = '\0';
            sub++;

            struct redisCommand *cmd = ACLLookupCommand(copy);

            /* 检查命令是否存在。我们无法检查
             * first-arg 是否有效。 */
            if (cmd == NULL) {
                zfree(copy);
                errno = ENOENT;
                return C_ERR;
            }

            /* 我们不支持允许子命令的 first-arg */
            if (cmd->parent) {
                zfree(copy);
                errno = ECHILD;
                return C_ERR;
            }

            /* 子命令不能为空，因此 DEBUG| 之类的当然是语法错误。 */
            if (strlen(sub) == 0) {
                zfree(copy);
                errno = EINVAL;
                return C_ERR;
            }

            if (cmd->subcommands_dict) {
                /* 如果用户试图允许一个有效的子命令，我们只需添加其唯一 ID */
                cmd = ACLLookupCommand(op+1);
                if (cmd == NULL) {
                    zfree(copy);
                    errno = ENOENT;
                    return C_ERR;
                }
                ACLChangeSelectorPerm(selector,cmd,1);
            } else {
                /* 如果用户试图使用 ACL 机制阻止 SELECT 但允许 SELECT 0，
                 * 或阻止 DEBUG 但允许 DEBUG OBJECT（DEBUG 子命令目前不被视为
                 * 子命令），我们使用 allowed_firstargs 机制。 */

                /* 将 first-arg 添加到有效列表中。 */
                serverLog(LL_WARNING, "Deprecation warning: Allowing a first arg of an otherwise "
                                      "blocked command is a misuse of ACL and may get disabled "
                                      "in the future (offender: +%s)", op+1);
                ACLAddAllowedFirstArg(selector,cmd->id,sub);
            }
            ACLUpdateCommandRules(selector,op+1,1);
            zfree(copy);
        }
    } else if (op[0] == '-' && op[1] != '@') {
        struct redisCommand *cmd = ACLLookupCommand(op+1);
        if (cmd == NULL) {
            errno = ENOENT;
            return C_ERR;
        }
        ACLChangeSelectorPerm(selector,cmd,0);
        ACLUpdateCommandRules(selector,cmd->fullname,0);
    } else if ((op[0] == '+' || op[0] == '-') && op[1] == '@') {
        int bitval = op[0] == '+' ? 1 : 0;
        if (ACLSetSelectorCategory(selector,op+1,bitval) == C_ERR) {
            errno = ENOENT;
            return C_ERR;
        }
    } else {
        errno = EINVAL;
        return C_ERR;
    }
    return C_OK;
}

/* 根据字符串 "op" 设置用户属性。以下描述了不同字符串的作用：
 *
 * on           启用用户：可以以此用户身份进行认证。
 * off          禁用用户：不再可以此用户身份进行认证，
 *              但已认证的连接仍可正常工作。
 * skip-sanitize-payload    跳过 RESTORE dump-payload 的净化。
 * sanitize-payload         净化 RESTORE dump-payload（默认）。
 * ><password>  将此密码添加到用户的有效密码列表。
 *              例如 >mypass 将 "mypass" 添加到列表中。
 *              此指令清除 "nopass" 标志（见后文）。
 * #<hash>      将此密码哈希添加到用户的有效哈希列表。
 *              如果您之前已计算了哈希且不想以明文存储，这很有用。
 *              此指令清除 "nopass" 标志（见后文）。
 * <<password>  从有效密码列表中移除此密码。
 * !<hash>      从有效密码列表中移除此哈希密码。
 *              当您想仅通过哈希移除密码而不知道其明文版本时很有用。
 * nopass       移除用户所有已设置的密码，并标记为不需要密码：
 *              这意味着任何密码都可以用于此用户认证。
 *              如果此指令用于默认用户，每个新连接将立即以
 *              默认用户身份认证，无需任何显式 AUTH 命令。
 *              注意 "resetpass" 指令将清除该状态。
 * resetpass    清空允许的密码列表，并移除 "nopass" 状态。
 *              执行 "resetpass" 后用户没有关联的密码，
 *              除了添加密码（或稍后设置为 "nopass"）外无法认证。
 * reset        执行以下操作：resetpass、resetkeys、resetchannels、
 *              allchannels（如果设置了 acl-pubsub-default）、off、
 *              clearselectors、-@all。用户恢复到创建后的初始状态。
 * (<options>)  使用括号内指定的选项创建新选择器并附加到用户。
 *              每个选项应以空格分隔。第一个字符必须是 (，
 *              最后一个字符必须是 )。
 * clearselectors          移除当前附加的所有选择器。
 *                         注意这不会更改 "root" 用户权限，
 *                         即直接应用于用户的权限（括号外的部分）。
 *
 * 选择器选项也可由此函数指定，此时会更新用户的根选择器。
 *
 * 'op' 字符串必须以 NULL 结尾。当调用者需要传递二进制数据时
 * （例如 >password 形式可能使用二进制密码），'oplen' 参数
 * 应指定 'op' 字符串的长度。否则可设置为 -1，函数将使用
 * strlen() 确定长度。
 *
 * 如果 'op' 字符串有意义且操作被理解，函数返回 C_OK。
 * 如果操作未知或有语法错误，返回 C_ERR。
 *
 * 返回错误时，errno 设置为以下值：
 *
 * EINVAL: 指定的操作码不被理解，或键/频道模式无效
 *         （包含不允许的字符）。
 * ENOENT: 通过 + 或 - 提供的命令名称或命令分类未知。
 * EEXIST: 在已添加 "*" 之后添加键模式。这几乎可以肯定是用户端的错误。
 * EISDIR: 在已添加 "*" 之后添加频道模式。这几乎可以肯定是用户端的错误。
 * ENODEV: 尝试从用户移除的密码不存在。
 * EBADMSG: 尝试添加的哈希不是有效的哈希。
 * ECHILD: 尝试允许子命令的特定第一个参数。
 */
int ACLSetUser(user *u, const char *op, ssize_t oplen) {
    /* 由于正在更改 ACL，旧的生成字符串现在无效 */
    if (u->acl_string) {
        decrRefCount(u->acl_string);
        u->acl_string = NULL;
    }

    if (oplen == -1) oplen = strlen(op);
    if (oplen == 0) return C_OK; /* 空字符串是无操作 */
    if (!strcasecmp(op,"on")) {
        u->flags |= USER_FLAG_ENABLED;
        u->flags &= ~USER_FLAG_DISABLED;
    } else if (!strcasecmp(op,"off")) {
        u->flags |= USER_FLAG_DISABLED;
        u->flags &= ~USER_FLAG_ENABLED;
    } else if (!strcasecmp(op,"skip-sanitize-payload")) {
        u->flags |= USER_FLAG_SANITIZE_PAYLOAD_SKIP;
        u->flags &= ~USER_FLAG_SANITIZE_PAYLOAD;
    } else if (!strcasecmp(op,"sanitize-payload")) {
        u->flags &= ~USER_FLAG_SANITIZE_PAYLOAD_SKIP;
        u->flags |= USER_FLAG_SANITIZE_PAYLOAD;
    } else if (!strcasecmp(op,"nopass")) {
        u->flags |= USER_FLAG_NOPASS;
        listEmpty(u->passwords);
    } else if (!strcasecmp(op,"resetpass")) {
        u->flags &= ~USER_FLAG_NOPASS;
        listEmpty(u->passwords);
    } else if (op[0] == '>' || op[0] == '#') {
        sds newpass;
        if (op[0] == '>') {
            newpass = ACLHashPassword((unsigned char*)op+1,oplen-1);
        } else {
            if (ACLCheckPasswordHash((unsigned char*)op+1,oplen-1) == C_ERR) {
                errno = EBADMSG;
                return C_ERR;
            }
            newpass = sdsnewlen(op+1,oplen-1);
        }

        listNode *ln = listSearchKey(u->passwords,newpass);
        /* 避免多次添加相同的密码。 */
        if (ln == NULL)
            listAddNodeTail(u->passwords,newpass);
        else
            sdsfree(newpass);
        u->flags &= ~USER_FLAG_NOPASS;
    } else if (op[0] == '<' || op[0] == '!') {
        sds delpass;
        if (op[0] == '<') {
            delpass = ACLHashPassword((unsigned char*)op+1,oplen-1);
        } else {
            if (ACLCheckPasswordHash((unsigned char*)op+1,oplen-1) == C_ERR) {
                errno = EBADMSG;
                return C_ERR;
            }
            delpass = sdsnewlen(op+1,oplen-1);
        }
        listNode *ln = listSearchKey(u->passwords,delpass);
        sdsfree(delpass);
        if (ln) {
            listDelNode(u->passwords,ln);
        } else {
            errno = ENODEV;
            return C_ERR;
        }
    } else if (op[0] == '(' && op[oplen - 1] == ')') {
        aclSelector *selector = aclCreateSelectorFromOpSet(op, oplen);
        if (!selector) {
            /* 未设置 errno，从内部错误传播。 */
            return C_ERR;
        }
        listAddNodeTail(u->selectors, selector);
        return C_OK;
    } else if (!strcasecmp(op,"clearselectors")) {
        listIter li;
        listNode *ln;
        listRewind(u->selectors,&li);
        /* 必须有一个根选择器 */
        serverAssert(listNext(&li));
        while((ln = listNext(&li))) {
            listDelNode(u->selectors, ln);
        }
        return C_OK;
    } else if (!strcasecmp(op,"reset")) {
        serverAssert(ACLSetUser(u,"resetpass",-1) == C_OK);
        serverAssert(ACLSetUser(u,"resetkeys",-1) == C_OK);
        serverAssert(ACLSetUser(u,"resetchannels",-1) == C_OK);
        if (server.acl_pubsub_default & SELECTOR_FLAG_ALLCHANNELS)
            serverAssert(ACLSetUser(u,"allchannels",-1) == C_OK);
        serverAssert(ACLSetUser(u,"off",-1) == C_OK);
        serverAssert(ACLSetUser(u,"sanitize-payload",-1) == C_OK);
        serverAssert(ACLSetUser(u,"clearselectors",-1) == C_OK);
        serverAssert(ACLSetUser(u,"-@all",-1) == C_OK);
    } else {
        aclSelector *selector = ACLUserGetRootSelector(u);
        if (ACLSetSelector(selector, op, oplen) == C_ERR) {
            return C_ERR;
        }
    }
    return C_OK;
}

/* 根据 ACLSetUser() 函数在错误时设置的 errno 值，返回错误描述。 */
const char *ACLSetUserStringError(void) {
    const char *errmsg = "Wrong format";
    if (errno == ENOENT)
        errmsg = "Unknown command or category name in ACL";
    else if (errno == EINVAL)
        errmsg = "Syntax error";
    else if (errno == EEXIST)
        errmsg = "Adding a pattern after the * pattern (or the "
                 "'allkeys' flag) is not valid and does not have any "
                 "effect. Try 'resetkeys' to start with an empty "
                 "list of patterns";
    else if (errno == EISDIR)
        errmsg = "Adding a pattern after the * pattern (or the "
                 "'allchannels' flag) is not valid and does not have any "
                 "effect. Try 'resetchannels' to start with an empty "
                 "list of channels";
    else if (errno == ENODEV)
        errmsg = "The password you are trying to remove from the user does "
                 "not exist";
    else if (errno == EBADMSG)
        errmsg = "The password hash must be exactly 64 characters and contain "
                 "only lowercase hexadecimal characters";
    else if (errno == EALREADY)
        errmsg = "Duplicate user found. A user can only be defined once in "
                 "config files";
    else if (errno == ECHILD)
        errmsg = "Allowing first-arg of a subcommand is not supported";
    return errmsg;
}

/* 创建默认用户，具有特殊权限。 */
user *ACLCreateDefaultUser(void) {
    user *new = ACLCreateUser("default",7);
    ACLSetUser(new,"+@all",-1);
    ACLSetUser(new,"~*",-1);
    ACLSetUser(new,"&*",-1);
    ACLSetUser(new,"on",-1);
    ACLSetUser(new,"nopass",-1);
    return new;
}

/* ACL 子系统的初始化。 */
void ACLInit(void) {
    Users = raxNew();
    UsersToLoad = listCreate();
    ACLInitCommandCategories();
    listSetMatchMethod(UsersToLoad, ACLListMatchLoadedUser);
    ACLLog = listCreate();
    DefaultUser = ACLCreateDefaultUser();
}

/* 检查用户名和密码对，如果有效返回 C_OK，
 * 否则返回 C_ERR 并将 errno 设置为：
 *
 *  EINVAL: 用户名密码不匹配。
 *  ENOENT: 指定的用户完全不存在。
 */
int ACLCheckUserCredentials(robj *username, robj *password) {
    user *u = ACLGetUserByName(username->ptr,sdslen(username->ptr));
    if (u == NULL) {
        errno = ENOENT;
        return C_ERR;
    }

    /* 禁用的用户无法登录。 */
    if (u->flags & USER_FLAG_DISABLED) {
        errno = EINVAL;
        return C_ERR;
    }

    /* 如果用户配置为不需要密码，此处直接通过。 */
    if (u->flags & USER_FLAG_NOPASS) return C_OK;

    /* 检查用户所有密码，至少匹配一个。 */
    listIter li;
    listNode *ln;
    listRewind(u->passwords,&li);
    sds hashed = ACLHashPassword(password->ptr,sdslen(password->ptr));
    while((ln = listNext(&li))) {
        sds thispass = listNodeValue(ln);
        if (!time_independent_strcmp(hashed, thispass, HASH_PASSWORD_LEN)) {
            sdsfree(hashed);
            return C_OK;
        }
    }
    sdsfree(hashed);

    /* 如果执行到这里，说明没有密码匹配。 */
    errno = EINVAL;
    return C_ERR;
}

/* 如果提供了 `err`，将其作为错误回复添加到客户端。
 * 否则，将标准 Auth 错误作为回复添加。 */
void addAuthErrReply(client *c, robj *err) {
    if (clientHasPendingReplies(c)) return;
    if (!err) {
        addReplyError(c, "-WRONGPASS invalid username-password pair or user is disabled.");
        return;
    }
    addReplyError(c, err->ptr);
}

/* 类似于 ACLCheckUserCredentials()，但如果用户/密码正确，
 * 连接将进入已认证状态并填充连接用户引用。
 *
 * 成功时返回 AUTH_OK（有效的用户名/密码对），否则返回 AUTH_ERR。 */
int checkPasswordBasedAuth(client *c, robj *username, robj *password) {
    if (ACLCheckUserCredentials(username,password) == C_OK) {
        c->authenticated = 1;
        c->user = ACLGetUserByName(username->ptr,sdslen(username->ptr));
        moduleNotifyUserChanged(c);
        return AUTH_OK;
    } else {
        addACLLogEntry(c,ACL_DENIED_AUTH,(c->flags & CLIENT_MULTI) ? ACL_LOG_CTX_MULTI : ACL_LOG_CTX_TOPLEVEL,0,username->ptr,NULL);
        return AUTH_ERR;
    }
}

/* 尝试认证用户 - 首先通过基于模块的认证，
 * 如果需要，再使用普通的基于密码的认证。
 * 返回以下代码之一：
 * AUTH_OK - 表示认证成功。
 * AUTH_ERR - 表示认证失败。
 * AUTH_BLOCKED - 表示模块认证正在通过阻塞实现进行中。
 */
int ACLAuthenticateUser(client *c, robj *username, robj *password, robj **err) {
    int result = checkModuleAuthentication(c, username, password, err);
    /* 如果认证未被任何模块处理，尝试普通的基于密码的认证。 */
    if (result == AUTH_NOT_HANDLED) {
        result = checkPasswordBasedAuth(c, username, password);
    }
    return result;
}

/* 出于 ACL 目的，每个用户都有一个位图，记录该用户允许执行的命令。
 * 为了填充位图，每个命令应有一个分配的 ID（用于索引位图）。
 * 此函数创建这样的 ID：使用顺序 ID，对相同的命令名称重用相同的 ID，
 * 这样在模块卸载后重新加载时命令能保留相同的 ID。
 *
 * 此函数不获取 'cmdname' SDS 字符串的所有权。
 * */
unsigned long ACLGetCommandID(sds cmdname) {
    sds lowername = sdsdup(cmdname);
    sdstolower(lowername);
    if (commandId == NULL) commandId = raxNew();
    void *id;
    if (raxFind(commandId,(unsigned char*)lowername,sdslen(lowername),&id)) {
        sdsfree(lowername);
        return (unsigned long)id;
    }
    raxInsert(commandId,(unsigned char*)lowername,strlen(lowername),
              (void*)nextid,NULL);
    sdsfree(lowername);
    unsigned long thisid = nextid;
    nextid++;

    /* 我们从不分配用户命令位图结构中的最后一位，
     * 这样稍后可以检查该位是否被设置，以了解用户的当前 ACL
     * 是以 +@all（添加所有可能命令并减去其他单个命令或分类）
     * 开头创建的，还是从零开始只添加命令和分类创建的
     * （默认不允许未来通过模块加载的命令）。
     * 这在使用 ACL SAVE 重写 ACL 时很有用。 */
    if (nextid == USER_COMMAND_BITS_COUNT-1) nextid++;
    return thisid;
}

/* 清除命令 ID 表并将 nextid 重置为 0。 */
void ACLClearCommandID(void) {
    if (commandId) raxFree(commandId);
    commandId = NULL;
    nextid = 0;
}

/* 根据名称返回用户名，如果用户不存在返回 NULL。 */
user *ACLGetUserByName(const char *name, size_t namelen) {
    void *myuser = NULL;
    raxFind(Users,(unsigned char*)name,namelen,&myuser);
    return myuser;
}

/* =============================================================================
 * ACL 权限检查
 * ==========================================================================*/

/* 检查选择器是否可以访问该键。
 *
 * 如果选择器可以访问该键，返回 ACL_OK，
 * 否则返回 ACL_DENIED_KEY。 */
static int ACLSelectorCheckKey(aclSelector *selector, const char *key, int keylen, int keyspec_flags) {
    /* 选择器可以访问任何键 */
    if (selector->flags & SELECTOR_FLAG_ALLKEYS) return ACL_OK;

    listIter li;
    listNode *ln;
    listRewind(selector->patterns,&li);

    int key_flags = 0;
    if (keyspec_flags & CMD_KEY_ACCESS) key_flags |= ACL_READ_PERMISSION;
    if (keyspec_flags & CMD_KEY_INSERT) key_flags |= ACL_WRITE_PERMISSION;
    if (keyspec_flags & CMD_KEY_DELETE) key_flags |= ACL_WRITE_PERMISSION;
    if (keyspec_flags & CMD_KEY_UPDATE) key_flags |= ACL_WRITE_PERMISSION;

    /* 对每个模式测试此键。 */
    while((ln = listNext(&li))) {
        keyPattern *pattern = listNodeValue(ln);
        if ((pattern->flags & key_flags) != key_flags)
            continue;
        size_t plen = sdslen(pattern->pattern);
        if (stringmatchlen(pattern->pattern,plen,key,keylen,0))
            return ACL_OK;
    }
    return ACL_DENIED_KEY;
}

/* 检查提供的选择器是否具有标志中指定的对键空间中所有键的访问权限。
 * 例如，CMD_KEY_READ 访问需要授予选择器 '%R~*'、'~*' 或 allkeys。
 * 如果此选择器满足所有访问标志返回 1，否则返回 0。
 */
static int ACLSelectorHasUnrestrictedKeyAccess(aclSelector *selector, int flags) {
    /* 选择器可以访问任何键 */
    if (selector->flags & SELECTOR_FLAG_ALLKEYS) return 1;

    listIter li;
    listNode *ln;
    listRewind(selector->patterns,&li);

    int access_flags = 0;
    if (flags & CMD_KEY_ACCESS) access_flags |= ACL_READ_PERMISSION;
    if (flags & CMD_KEY_INSERT) access_flags |= ACL_WRITE_PERMISSION;
    if (flags & CMD_KEY_DELETE) access_flags |= ACL_WRITE_PERMISSION;
    if (flags & CMD_KEY_UPDATE) access_flags |= ACL_WRITE_PERMISSION;

    /* 对每个模式测试此键。 */
    while((ln = listNext(&li))) {
        keyPattern *pattern = listNodeValue(ln);
        if ((pattern->flags & access_flags) != access_flags)
            continue;
        if (!strcmp(pattern->pattern,"*")) {
           return 1;
       }
    }
    return 0;
}

/* 对照提供的频道列表检查频道。is_pattern 参数仅在订阅时使用
 * （发布时不用），控制输入频道是作为频道模式（如 PSUBSCRIBE）
 * 还是普通频道名称（如 SUBSCRIBE）进行评估。
 *
 * 注意 PUBLISH 或 SUBSCRIBE 中的普通频道名称可以匹配 ACL 频道模式，
 * 但 PSUBSCRIBE 中提供的模式只能与 ACL 模式进行字面匹配
 * （使用纯字符串比较）。 */
static int ACLCheckChannelAgainstList(list *reference, const char *channel, int channellen, int is_pattern) {
    listIter li;
    listNode *ln;

    listRewind(reference, &li);
    while((ln = listNext(&li))) {
        sds pattern = listNodeValue(ln);
        size_t plen = sdslen(pattern);
        /* 频道模式与列表中的频道进行字面匹配。
         * 普通频道执行模式匹配。 */
        if ((is_pattern && !strcmp(pattern,channel)) || 
            (!is_pattern && stringmatchlen(pattern,plen,channel,channellen,0)))
        {
            return ACL_OK;
        }
    }
    return ACL_DENIED_CHANNEL;
}

/* 为防止对 getKeysResult 的重复调用，在各选择器调用之间维护一个缓存。 */
typedef struct {
    int keys_init;
    getKeysResult keys;
} aclKeyResultCache;

void initACLKeyResultCache(aclKeyResultCache *cache) {
    cache->keys_init = 0;
}

void cleanupACLKeyResultCache(aclKeyResultCache *cache) {
    if (cache->keys_init) getKeysFreeResult(&(cache->keys));
}

/* 根据与指定选择器关联的 ACL 检查命令是否准备好执行。
 *
 * 如果选择器可以执行该命令，返回 ACL_OK，
 * 否则返回 ACL_DENIED_CMD、ACL_DENIED_KEY 或 ACL_DENIED_CHANNEL：
 * 第一个表示选择器不被允许运行该命令，
 * 第二和第三个表示选择器试图访问不在指定模式中的键或频道。 */
static int ACLSelectorCheckCmd(aclSelector *selector, struct redisCommand *cmd, robj **argv, int argc, int *keyidxptr, aclKeyResultCache *cache) {
    uint64_t id = cmd->id;
    int ret;
    if (!(selector->flags & SELECTOR_FLAG_ALLCOMMANDS) && !(cmd->flags & CMD_NO_AUTH)) {
        /* 如果位未设置，需要进一步检查，
         * 因为命令可能仅在指定特定第一个参数时才被允许。 */
        if (ACLGetSelectorCommandBit(selector,id) == 0) {
            /* 检查第一个参数是否匹配。 */
            if (argc < 2 ||
                selector->allowed_firstargs == NULL ||
                selector->allowed_firstargs[id] == NULL)
            {
                return ACL_DENIED_CMD;
            }

            long subid = 0;
            while (1) {
                if (selector->allowed_firstargs[id][subid] == NULL)
                    return ACL_DENIED_CMD;
                int idx = cmd->parent ? 2 : 1;
                if (!strcasecmp(argv[idx]->ptr,selector->allowed_firstargs[id][subid]))
                    break; /* 找到第一个参数匹配。在此停止。 */
                subid++;
            }
        }
    }

    /* 检查用户是否可以执行明确触及命令参数中提及的键的命令。 */
    if (!(selector->flags & SELECTOR_FLAG_ALLKEYS) && doesCommandHaveKeys(cmd)) {
        if (!(cache->keys_init)) {
            cache->keys = (getKeysResult) GETKEYS_RESULT_INIT;
            getKeysFromCommandWithSpecs(cmd, argv, argc, GET_KEYSPEC_DEFAULT, &(cache->keys));
            cache->keys_init = 1;
        }
        getKeysResult *result = &(cache->keys);
        keyReference *resultidx = result->keys;
        for (int j = 0; j < result->numkeys; j++) {
            int idx = resultidx[j].pos;
            ret = ACLSelectorCheckKey(selector, argv[idx]->ptr, sdslen(argv[idx]->ptr), resultidx[j].flags);
            if (ret != ACL_OK) {
                if (keyidxptr) *keyidxptr = resultidx[j].pos;
                return ret;
            }
        }
    }

    /* 检查用户是否可以执行明确触及命令参数中提及的频道的命令 */
    const int channel_flags = CMD_CHANNEL_PUBLISH | CMD_CHANNEL_SUBSCRIBE;
    if (!(selector->flags & SELECTOR_FLAG_ALLCHANNELS) && doesCommandHaveChannelsWithFlags(cmd, channel_flags)) {
        getKeysResult channels = (getKeysResult) GETKEYS_RESULT_INIT;
        getChannelsFromCommand(cmd, argv, argc, &channels);
        keyReference *channelref = channels.keys;
        for (int j = 0; j < channels.numkeys; j++) {
            int idx = channelref[j].pos;
            if (!(channelref[j].flags & channel_flags)) continue;
            int is_pattern = channelref[j].flags & CMD_CHANNEL_PATTERN;
            int ret = ACLCheckChannelAgainstList(selector->channels, argv[idx]->ptr, sdslen(argv[idx]->ptr), is_pattern);
            if (ret != ACL_OK) {
                if (keyidxptr) *keyidxptr = channelref[j].pos;
                getKeysFreeResult(&channels);
                return ret;
            }
        }
        getKeysFreeResult(&channels);
    }
    return ACL_OK;
}

/* 根据与指定用户关联的 ACL 和 keyspec 访问标志，
 * 检查客户端是否可以访问该键。
 *
 * 如果用户可以访问该键，返回 ACL_OK，
 * 否则返回 ACL_DENIED_KEY。 */
int ACLUserCheckKeyPerm(user *u, const char *key, int keylen, int flags) {
    listIter li;
    listNode *ln;

    /* 如果没有关联用户，连接可以运行任何命令。 */
    if (u == NULL) return ACL_OK;

    /* 检查所有选择器 */
    listRewind(u->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        if (ACLSelectorCheckKey(s, key, keylen, flags) == ACL_OK) {
            return ACL_OK;
        }
    }
    return ACL_DENIED_KEY;
}

/* 检查用户是否可以执行给定命令，并附加限制条件：
 * 用户还必须具有标志中指定的对键空间中任何键的访问权限。
 * 例如，CMD_KEY_READ 访问需要在命令所需权限之外额外授予
 * '%R~*'、'~*' 或 allkeys。如果用户有访问权限返回 1，否则返回 0。
 */
int ACLUserCheckCmdWithUnrestrictedKeyAccess(user *u, struct redisCommand *cmd, robj **argv, int argc, int flags) {
    listIter li;
    listNode *ln;
    int local_idxptr;

    /* 如果没有关联用户，连接可以运行任何命令。 */
    if (u == NULL) return 1;

    /* 对于多个选择器，在选择器调用之间缓存键结果以防止重复查找。 */
    aclKeyResultCache cache;
    initACLKeyResultCache(&cache);

    /* 顺序检查每个选择器 */
    listRewind(u->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        int acl_retval = ACLSelectorCheckCmd(s, cmd, argv, argc, &local_idxptr, &cache);
        if (acl_retval == ACL_OK && ACLSelectorHasUnrestrictedKeyAccess(s, flags)) {
            cleanupACLKeyResultCache(&cache);
            return 1;
        }
    }
    cleanupACLKeyResultCache(&cache);
    return 0;
}

/* 根据与指定用户关联的 ACL 检查客户端是否可以访问该频道。
 *
 * 如果用户可以访问该频道，返回 ACL_OK，
 * 否则返回 ACL_DENIED_CHANNEL。 */
int ACLUserCheckChannelPerm(user *u, sds channel, int is_pattern) {
    listIter li;
    listNode *ln;

    /* 如果没有关联用户，连接可以运行任何命令。 */
    if (u == NULL) return ACL_OK;

    /* 检查所有选择器 */
    listRewind(u->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        /* 选择器可以运行任何频道 */
        if (s->flags & SELECTOR_FLAG_ALLCHANNELS) return ACL_OK;

        /* 否则，遍历选择器列表并检查每个频道 */
        if (ACLCheckChannelAgainstList(s->channels, channel, sdslen(channel), is_pattern) == ACL_OK) {
            return ACL_OK;
        }
    }
    return ACL_DENIED_CHANNEL;
}

/* 较低级别的 API，检查指定用户是否能执行给定命令。
 *
 * 如果命令未通过 ACL 检查，idxptr 将设置为导致失败的第一个 argv 条目：
 * 如果命令本身失败则为 0，否则为导致失败的键/频道的索引。 */
int ACLCheckAllUserCommandPerm(user *u, struct redisCommand *cmd, robj **argv, int argc, int *idxptr) {
    listIter li;
    listNode *ln;

    /* 如果没有关联用户，连接可以运行任何命令。 */
    if (u == NULL) return ACL_OK;

    /* 我们需要选择一个错误进行记录，选择逻辑如下：
     * 1) 如果没有选择器可以执行该命令，返回命令错误。
     * 2) 返回没有选择器能匹配的最后一个键或频道。 */
    int relevant_error = ACL_DENIED_CMD;
    int local_idxptr = 0, last_idx = 0;

    /* 对于多个选择器，在选择器调用之间缓存键结果以防止重复查找。 */
    aclKeyResultCache cache;
    initACLKeyResultCache(&cache);

    /* 顺序检查每个选择器 */
    listRewind(u->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        int acl_retval = ACLSelectorCheckCmd(s, cmd, argv, argc, &local_idxptr, &cache);
        if (acl_retval == ACL_OK) {
            cleanupACLKeyResultCache(&cache);
            return ACL_OK;
        }
        if (acl_retval > relevant_error ||
            (acl_retval == relevant_error && local_idxptr > last_idx))
        {
            relevant_error = acl_retval;
            last_idx = local_idxptr;
        }
    }

    *idxptr = last_idx;
    cleanupACLKeyResultCache(&cache);
    return relevant_error;
}

/* 检查客户端是否可以执行排队命令的高级 API */
int ACLCheckAllPerm(client *c, int *idxptr) {
    return ACLCheckAllUserCommandPerm(c->user, c->cmd, c->argv, c->argc, idxptr);
}

/* 如果 'new' 可以访问 'original' 的所有频道则返回 NULL；
   否则返回新用户可以访问的频道列表 */
list *getUpcomingChannelList(user *new, user *original) {
    listIter li, lpi;
    listNode *ln, *lpn;

    /* 优化：检查是否有选择器拥有所有频道权限。 */
    listRewind(new->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        if (s->flags & SELECTOR_FLAG_ALLCHANNELS) return NULL;
    }

    /* 接下来，检查新的频道列表是否是原始列表的严格超集。
     * 通过创建新用户中所有频道的"即将到来"列表，
     * 并将现有频道逐一对照检查来实现。 */
    list *upcoming = listCreate();
    listRewind(new->selectors,&li);
    while((ln = listNext(&li))) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        listRewind(s->channels, &lpi);
        while((lpn = listNext(&lpi))) {
            listAddNodeTail(upcoming, listNodeValue(lpn));
        }
    }

    int match = 1;
    listRewind(original->selectors,&li);
    while((ln = listNext(&li)) && match) {
        aclSelector *s = (aclSelector *) listNodeValue(ln);
        /* 如果原始选择器中有任何一个拥有 all-channels 权限，
         * 但新的没有（这在此函数前面已检查），则新列表
         * 不是原始列表的严格超集。 */
        if (s->flags & SELECTOR_FLAG_ALLCHANNELS) {
            match = 0;
            break;
        }
        listRewind(s->channels, &lpi);
        while((lpn = listNext(&lpi)) && match) {
            if (!listSearchKey(upcoming, listNodeValue(lpn))) {
                match = 0;
                break;
            }
        }
    }

    if (match) {
        /* 所有频道都已匹配，不需要终止客户端。 */
        listRelease(upcoming);
        return NULL;
    }

    return upcoming;
}

/* 检查客户端是否应被终止，因为它订阅的频道在过去是允许的，
 * 但不在 `upcoming` 频道列表中。 */
int ACLShouldKillPubsubClient(client *c, list *upcoming) {
    robj *o;
    int kill = 0;

    if (getClientType(c) == CLIENT_TYPE_PUBSUB) {
        /* 检查模式违规 */
        dictIterator *di = dictGetIterator(c->pubsub_patterns);
        dictEntry *de;
        while (!kill && ((de = dictNext(di)) != NULL)) {
            o = dictGetKey(de);
            int res = ACLCheckChannelAgainstList(upcoming, o->ptr, sdslen(o->ptr), 1);
            kill = (res == ACL_DENIED_CHANNEL);
        }
        dictReleaseIterator(di);

        /* 检查频道违规 */
        if (!kill) {
            /* 检查全局频道违规 */
            di = dictGetIterator(c->pubsub_channels);

            while (!kill && ((de = dictNext(di)) != NULL)) {
                o = dictGetKey(de);
                int res = ACLCheckChannelAgainstList(upcoming, o->ptr, sdslen(o->ptr), 0);
                kill = (res == ACL_DENIED_CHANNEL);
            }
            dictReleaseIterator(di);
        }
        if (!kill) {
            /* 检查分片频道违规 */
            di = dictGetIterator(c->pubsubshard_channels);
            while (!kill && ((de = dictNext(di)) != NULL)) {
                o = dictGetKey(de);
                int res = ACLCheckChannelAgainstList(upcoming, o->ptr, sdslen(o->ptr), 0);
                kill = (res == ACL_DENIED_CHANNEL);
            }
            dictReleaseIterator(di);
        }

        if (kill) {
            return 1;
        }
    }
    return 0;
}

/* 检查用户现有的 pub/sub 客户端是否违反了通过 upcoming 参数指定的
 * ACL pub/sub 权限，如果是则终止它们。 */
void ACLKillPubsubClientsIfNeeded(user *new, user *original) {
    /* 如果没有订阅者则不执行任何操作。 */
    if (pubsubTotalSubscriptions() == 0)
        return;

    list *channels = getUpcomingChannelList(new, original);
    /* 如果新用户的 pubsub 权限是原始权限的严格超集，提前返回。 */
    if (!channels)
        return;

    listIter li;
    listNode *ln;

    /* 权限已更改，需要遍历所有客户端并断开不再有效的连接。
     * 扫描所有已连接客户端以找到用户的 pub/sub。 */
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        if (c->user != original)
            continue;
        if (ACLShouldKillPubsubClient(c, channels))
            deauthenticateAndCloseClient(c);
    }

    listRelease(channels);
}

/* =============================================================================
 * ACL 加载/保存函数
 * ==========================================================================*/


/* 选择器定义应作为单个参数发送，但我们会宽容地尝试查找分散在
 * 多个参数中的选择器定义，因为这使 ACL SETUSER 和从配置文件
 * 加载时的用户体验更简单。
 *
 * 此函数接收一个 ACL 操作符数组（不包括用户名），
 * 并合并分散在多个参数中的选择器操作。返回值是一个新的 SDS 数组，
 * 长度设置为传入的 merged_argc。未修改的参数仍会被复制。
 * 如果有未匹配的括号，返回 NULL，
 * invalid_idx 设置为开头括号所在参数的索引。 */
sds *ACLMergeSelectorArguments(sds *argv, int argc, int *merged_argc, int *invalid_idx) {
    *merged_argc = 0;
    int open_bracket_start = -1;

    sds *acl_args = (sds *) zmalloc(sizeof(sds) * argc);

    sds selector = NULL;
    for (int j = 0; j < argc; j++) {
        char *op = argv[j];

        if (open_bracket_start == -1 &&
            (op[0] == '(' && op[sdslen(op) - 1] != ')')) {
            selector = sdsdup(argv[j]);
            open_bracket_start = j;
            continue;
        }

        if (open_bracket_start != -1) {
            selector = sdscatfmt(selector, " %s", op);
            if (op[sdslen(op) - 1] == ')') {
                open_bracket_start = -1;
                acl_args[*merged_argc] = selector;                        
                (*merged_argc)++;
            }
            continue;
        }

        acl_args[*merged_argc] = sdsdup(argv[j]);
        (*merged_argc)++;
    }

    if (open_bracket_start != -1) {
        for (int i = 0; i < *merged_argc; i++) sdsfree(acl_args[i]);
        zfree(acl_args);
        sdsfree(selector);
        if (invalid_idx) *invalid_idx = open_bracket_start;
        return NULL;
    }

    return acl_args;
}

/* 接收已按空格分割的 acl 字符串并添加到给定用户。
 * 如果用户对象为 NULL，将使用给定用户名创建一个新用户。
 *
 * 如果 ACL 字符串无法解析，返回一个 SDS 字符串形式的错误。
 */
sds ACLStringSetUser(user *u, sds username, sds *argv, int argc) {
    serverAssert(u != NULL || username != NULL);

    sds error = NULL;

    int merged_argc = 0, invalid_idx = 0;
    sds *acl_args = ACLMergeSelectorArguments(argv, argc, &merged_argc, &invalid_idx);

    if (!acl_args) {
        return sdscatfmt(sdsempty(),
                         "Unmatched parenthesis in acl selector starting "
                         "at '%s'.", (char *) argv[invalid_idx]);
    }

    /* 创建一个临时用户，在应用到现有用户或创建新用户之前，
     * 对其进行所有更改的验证和暂存。如果所有参数都有效，
     * 用户参数将一起应用。如果有任何错误，则不应用任何更改。 */
    user *tempu = ACLCreateUnlinkedUser();
    if (u) {
        ACLCopyUser(tempu, u);
    }

    for (int j = 0; j < merged_argc; j++) {
        if (ACLSetUser(tempu,acl_args[j],(ssize_t) sdslen(acl_args[j])) != C_OK) {
            const char *errmsg = ACLSetUserStringError();
            error = sdscatfmt(sdsempty(),
                              "Error in ACL SETUSER modifier '%s': %s",
                              (char*)acl_args[j], errmsg);
            goto cleanup;
        }
    }

    /* 如果（部分）频道权限被撤销，已认证用户的现有 pub/sub 客户端
     * 可能需要断开连接。 */
    if (u) {
        ACLKillPubsubClientsIfNeeded(tempu, u);
    }

    /* 用上面修改的临时用户覆盖该用户。 */
    if (!u) {
        u = ACLCreateUser(username,sdslen(username));
    }
    serverAssert(u != NULL);

    ACLCopyUser(u, tempu);

cleanup:
    ACLFreeUser(tempu);
    for (int i = 0; i < merged_argc; i++) {
        sdsfree(acl_args[i]);
    }
    zfree(acl_args);

    return error;
}

/* 给定以如下形式描述用户的参数向量：
 *
 *      user <username> ... ACL 规则和标志 ...
 *
 * 此函数进行验证，如果语法有效则将用户定义追加到列表以供后续加载。
 *
 * 规则会进行有效性测试，如果有明显的语法错误，
 * 函数返回 C_ERR 且不执行任何操作，否则返回 C_OK
 * 并将用户追加到列表。
 *
 * 注意此函数无法在找不到命令时停止，这种情况下错误将在稍后发出，
 * 因为某些命令可能在模块加载后才定义。
 *
 * 检测到错误并返回 C_ERR 时，函数通过引用（如果未设置为 NULL）
 * 填充 argc_err 参数为导致错误的 argv 向量的索引。 */
int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err) {
    if (argc < 2 || strcasecmp(argv[0],"user")) {
        if (argc_err) *argc_err = 0;
        return C_ERR;
    }

    if (listSearchKey(UsersToLoad, argv[1])) {
        if (argc_err) *argc_err = 1;
        errno = EALREADY;
        return C_ERR; 
    }

    /* 尝试处理前先合并选择器 */
    int merged_argc;
    sds *acl_args = ACLMergeSelectorArguments(argv + 2, argc - 2, &merged_argc, argc_err);

    if (!acl_args) {
        return C_ERR;
    }

    /* 尝试在假用户上应用用户规则以查看它们是否实际有效。 */
    user *fakeuser = ACLCreateUnlinkedUser();

    for (int j = 0; j < merged_argc; j++) {
        if (ACLSetUser(fakeuser,acl_args[j],sdslen(acl_args[j])) == C_ERR) {
            if (errno != ENOENT) {
                ACLFreeUser(fakeuser);
                if (argc_err) *argc_err = j;
                for (int i = 0; i < merged_argc; i++) sdsfree(acl_args[i]);
                zfree(acl_args);
                return C_ERR;
            }
        }
    }

    /* 规则看起来有效，将用户追加到列表。 */
    sds *copy = zmalloc(sizeof(sds)*(merged_argc + 2));
    copy[0] = sdsdup(argv[1]);
    for (int j = 0; j < merged_argc; j++) copy[j+1] = sdsdup(acl_args[j]);
    copy[merged_argc + 1] = NULL;
    listAddNodeTail(UsersToLoad,copy);
    ACLFreeUser(fakeuser);
    for (int i = 0; i < merged_argc; i++) sdsfree(acl_args[i]);
    zfree(acl_args);
    return C_OK;
}

/* 此函数将加载通过 ACLAppendUserForLoading() 追加到服务器配置的用户。
 * 加载错误时将记录错误并返回 C_ERR，否则返回 C_OK。 */
int ACLLoadConfiguredUsers(void) {
    listIter li;
    listNode *ln;
    listRewind(UsersToLoad,&li);
    while ((ln = listNext(&li)) != NULL) {
        sds *aclrules = listNodeValue(ln);
        sds username = aclrules[0];

        if (ACLStringHasSpaces(aclrules[0],sdslen(aclrules[0]))) {
            serverLog(LL_WARNING,"Spaces not allowed in ACL usernames");
            return C_ERR;
        }

        user *u = ACLCreateUser(username,sdslen(username));
        if (!u) {
            /* 唯一有效的重复用户是默认用户。 */
            serverAssert(!strcmp(username, "default"));
            u = ACLGetUserByName("default",7);
            ACLSetUser(u,"reset",-1);
        }

        /* 加载此用户定义的每条规则。 */
        for (int j = 1; aclrules[j]; j++) {
            if (ACLSetUser(u,aclrules[j],sdslen(aclrules[j])) != C_OK) {
                const char *errmsg = ACLSetUserStringError();
                serverLog(LL_WARNING,"Error loading ACL rule '%s' for "
                                     "the user named '%s': %s",
                          aclrules[j],aclrules[0],errmsg);
                return C_ERR;
            }
        }

        /* 配置中存在禁用的用户可能是错误，
         * 发出警告但不向调用者返回错误。 */
        if (u->flags & USER_FLAG_DISABLED) {
            serverLog(LL_NOTICE, "The user '%s' is disabled (there is no "
                                 "'on' modifier in the user description). Make "
                                 "sure this is not a configuration error.",
                      aclrules[0]);
        }
    }
    return C_OK;
}

/* 此函数从指定文件名加载 ACL：每行都会被验证，
 * 应为空行或使用 redis.conf 配置或 ACL 文件中指定用户的格式：
 *
 *  user <username> ... rules ...
 *
 * 注意此函数将 '#' 开头的注释视为错误，因为 ACL 文件旨在被重写，
 * 注释在重写后会丢失。但允许空行以避免过于严格。
 *
 * 实现 ACL LOAD（使用此函数）的一个重要部分是避免在 ACL 文件
 * 因某种原因无效时以损坏的规则结束，因此函数会在加载每个用户
 * 前尝试验证规则。对于发现的每个损坏行，函数将收集错误消息。
 *
 * 重要：如果至少有一个错误，将不会加载任何内容，
 * 规则将保持原样。
 *
 * 过程结束时，如果整个文件中未发现错误则返回 NULL。
 * 否则返回一个 SDS 字符串，在一行中描述所有发现的问题。 */
sds ACLLoadFromFile(const char *filename) {
    FILE *fp;
    char buf[1024];

    /* 打开 ACL 文件 */
    if ((fp = fopen(filename,"r")) == NULL) {
        sds errors = sdscatprintf(sdsempty(),
            "Error loading ACLs, opening file '%s': %s",
            filename, strerror(errno));
        return errors;
    }

    /* 将整个文件作为单个字符串加载到内存中 */
    sds acls = sdsempty();
    while(fgets(buf,sizeof(buf),fp) != NULL)
        acls = sdscat(acls,buf);
    fclose(fp);

    /* 将文件分割为行并尝试加载每一行 */
    int totlines;
    sds *lines, errors = sdsempty();
    lines = sdssplitlen(acls,strlen(acls),"\n",1,&totlines);
    sdsfree(acls);

    /* 在新的 Users 基数树实例中进行所有加载，
     * 这样如果加载 ACL 文件出错可以回滚到旧版本。 */
    rax *old_users = Users;
    Users = raxNew();

    /* 加载文件的每一行 */
    for (int i = 0; i < totlines; i++) {
        sds *argv;
        int argc;
        int linenum = i+1;

        lines[i] = sdstrim(lines[i]," \t\r\n");

        /* 跳过空行 */
        if (lines[i][0] == '\0') continue;

        /* 分割为参数 */
        argv = sdssplitlen(lines[i],sdslen(lines[i])," ",1,&argc);
        if (argv == NULL) {
            errors = sdscatprintf(errors,
                     "%s:%d: unbalanced quotes in acl line. ",
                     server.acl_filename, linenum);
            continue;
        }

        /* 如果结果命令向量为空则跳过此行 */
        if (argc == 0) {
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* 行应以 "user" 关键字开头 */
        if (strcmp(argv[0],"user") || argc < 2) {
            errors = sdscatprintf(errors,
                     "%s:%d should start with user keyword followed "
                     "by the username. ", server.acl_filename,
                     linenum);
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* 用户名中不允许有空格 */
        if (ACLStringHasSpaces(argv[1],sdslen(argv[1]))) {
            errors = sdscatprintf(errors,
                     "'%s:%d: username '%s' contains invalid characters. ",
                     server.acl_filename, linenum, argv[1]);
            sdsfreesplitres(argv,argc);
            continue;
        }

        user *u = ACLCreateUser(argv[1],sdslen(argv[1]));

        /* 如果用户已存在，我们认为是错误并中止。 */
        if (!u) {
            errors = sdscatprintf(errors,"WARNING: Duplicate user '%s' found on line %d. ", argv[1], linenum);
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* 最后处理选项并验证它们能否干净地应用到用户。
         * 如果任何选项应用失败，其他值也不会被应用，
         * 因为所有待处理的更改将被丢弃。 */
        int merged_argc;
        sds *acl_args = ACLMergeSelectorArguments(argv + 2, argc - 2, &merged_argc, NULL);
        if (!acl_args) {
            errors = sdscatprintf(errors,
                    "%s:%d: Unmatched parenthesis in selector definition.",
                    server.acl_filename, linenum);
        }

        int syntax_error = 0;
        for (int j = 0; j < merged_argc; j++) {
            acl_args[j] = sdstrim(acl_args[j],"\t\r\n");
            if (ACLSetUser(u,acl_args[j],sdslen(acl_args[j])) != C_OK) {
                const char *errmsg = ACLSetUserStringError();
                if (errno == ENOENT) {
                    /* 对于缺失的命令，我们打印更多信息，
                     * 因为它不应包含任何敏感信息。 */
                    errors = sdscatprintf(errors,
                            "%s:%d: Error in applying operation '%s': %s. ",
                            server.acl_filename, linenum, acl_args[j], errmsg);
                } else if (syntax_error == 0) {
                    /* 对于所有其他错误，只打印遇到的第一个错误，
                     * 因为它可能影响后续操作。 */
                    errors = sdscatprintf(errors,
                            "%s:%d: %s. ",
                            server.acl_filename, linenum, errmsg);
                    syntax_error = 1;
                }
            }
        }

        for (int i = 0; i < merged_argc; i++) sdsfree(acl_args[i]);
        zfree(acl_args);

        /* 仅在目前没有错误时将规则应用到新用户集，
         * 否则是无用的，因为我们反正要丢弃新用户集。 */
        if (sdslen(errors) != 0) {
            sdsfreesplitres(argv,argc);
            continue;
        }

        sdsfreesplitres(argv,argc);
    }

    sdsfreesplitres(lines,totlines);

    /* 检查是否发现错误并做出相应反应。 */
    if (sdslen(errors) == 0) {
        /* 默认用户指针在不同地方被引用：与其替换这些引用，
         * 将新的默认用户配置复制到旧的中更为简单。 */
        user *new_default = ACLGetUserByName("default",7);
        if (!new_default) {
            new_default = ACLCreateDefaultUser();
        }

        ACLCopyUser(DefaultUser,new_default);
        ACLFreeUser(new_default);
        raxInsert(Users,(unsigned char*)"default",7,DefaultUser,NULL);
        raxRemove(old_users,(unsigned char*)"default",7,NULL);

        /* 如果有一些订阅者，需要检查是否需要断开一些客户端。 */
        rax *user_channels = NULL;
        if (pubsubTotalSubscriptions() > 0) {
            user_channels = raxNew();
        }

        listIter li;
        listNode *ln;

        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            /* MASTER 客户端可以执行任何操作（且 user = NULL），因此可以跳过 */
            if (c->flags & CLIENT_MASTER)
                continue;
            user *original = c->user;
            list *channels = NULL;
            user *new = ACLGetUserByName(c->user->name, sdslen(c->user->name));
            if (new && user_channels) {
                if (!raxFind(user_channels, (unsigned char*)(new->name), sdslen(new->name), (void**)&channels)) {
                    channels = getUpcomingChannelList(new, original);
                    raxInsert(user_channels, (unsigned char*)(new->name), sdslen(new->name), channels, NULL);
                }
            }
            /* 当新的频道列表为 NULL 时，意味着新用户的频道列表是旧用户列表的超集。 */
            if (!new || (channels && ACLShouldKillPubsubClient(c, channels))) {
                deauthenticateAndCloseClient(c);
                continue;
            }
            c->user = new;
        }

        if (user_channels)
            raxFreeWithCallback(user_channels, (void(*)(void*))listRelease);
        raxFreeWithCallback(old_users,(void(*)(void*))ACLFreeUser);
        sdsfree(errors);
        return NULL;
    } else {
        raxFreeWithCallback(Users,(void(*)(void*))ACLFreeUser);
        Users = old_users;
        errors = sdscat(errors,"WARNING: ACL errors detected, no change to the previously active ACL rules was performed");
        return errors;
    }
}

/* 将当前内存中的 ACL 生成副本到指定文件名。
 * 成功返回 C_OK，I/O 期间出错返回 C_ERR。
 * 返回 C_ERR 时会生成带有问题提示的日志。 */
int ACLSaveToFile(const char *filename) {
    sds acl = sdsempty();
    int fd = -1;
    sds tmpfilename = NULL;
    int retval = C_ERR;

    /* 生成包含新版本 ACL 文件的 SDS 字符串。 */
    raxIterator ri;
    raxStart(&ri,Users);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        user *u = ri.data;
        /* 以配置文件格式返回信息。 */
        sds user = sdsnew("user ");
        user = sdscatsds(user,u->name);
        user = sdscatlen(user," ",1);
        robj *descr = ACLDescribeUser(u);
        user = sdscatsds(user,descr->ptr);
        decrRefCount(descr);
        acl = sdscatsds(acl,user);
        acl = sdscatlen(acl,"\n",1);
        sdsfree(user);
    }
    raxStop(&ri);

    /* 使用新内容创建临时文件 */
    tmpfilename = sdsnew(filename);
    tmpfilename = sdscatfmt(tmpfilename,".tmp-%i-%I",
        (int) getpid(),commandTimeSnapshot());
    if ((fd = open(tmpfilename,O_WRONLY|O_CREAT,0644)) == -1) {
        serverLog(LL_WARNING,"Opening temp ACL file for ACL SAVE: %s",
            strerror(errno));
        goto cleanup;
    }

    /* 写入文件 */
    size_t offset = 0;
    while (offset < sdslen(acl)) {
        ssize_t written_bytes = write(fd,acl + offset,sdslen(acl) - offset);
        if (written_bytes <= 0) {
            if (errno == EINTR) continue;
            serverLog(LL_WARNING,"Writing ACL file for ACL SAVE: %s",
                strerror(errno));
            goto cleanup;
        }
        offset += written_bytes;
    }
    if (redis_fsync(fd) == -1) {
        serverLog(LL_WARNING,"Syncing ACL file for ACL SAVE: %s",
            strerror(errno));
        goto cleanup;
    }
    close(fd); fd = -1;

    /* 用新文件替换旧文件 */
    if (rename(tmpfilename,filename) == -1) {
        serverLog(LL_WARNING,"Renaming ACL file for ACL SAVE: %s",
            strerror(errno));
        goto cleanup;
    }
    if (fsyncFileDir(filename) == -1) {
        serverLog(LL_WARNING,"Syncing ACL directory for ACL SAVE: %s",
            strerror(errno));
        goto cleanup;
    }
    sdsfree(tmpfilename); tmpfilename = NULL;
    retval = C_OK; /* 如果执行到这里，说明一切正常 */

cleanup:
    if (fd != -1) close(fd);
    if (tmpfilename) unlink(tmpfilename);
    sdsfree(tmpfilename);
    sdsfree(acl);
    return retval;
}

/* 此函数在服务器已运行、模块已加载且准备好启动时调用，
 * 以从 redis.conf 中定义的待处理用户列表或 ACL 文件加载 ACL。
 * 如果用户试图混合使用两种加载方法，函数将直接以错误退出。 */
void ACLLoadUsersAtStartup(void) {
    if (server.acl_filename[0] != '\0' && listLength(UsersToLoad) != 0) {
        serverLog(LL_WARNING,
            "Configuring Redis with users defined in redis.conf and at "
            "the same setting an ACL file path is invalid. This setup "
            "is very likely to lead to configuration errors and security "
            "holes, please define either an ACL file or declare users "
            "directly in your redis.conf, but not both.");
        exit(1);
    }

    if (ACLLoadConfiguredUsers() == C_ERR) {
        serverLog(LL_WARNING,
            "Critical error while loading ACLs. Exiting.");
        exit(1);
    }

    if (server.acl_filename[0] != '\0') {
        sds errors = ACLLoadFromFile(server.acl_filename);
        if (errors) {
            serverLog(LL_WARNING,
                "Aborting Redis startup because of ACL errors: %s", errors);
            sdsfree(errors);
            exit(1);
        }
    }
}

/* =============================================================================
 * ACL 日志
 * ==========================================================================*/

#define ACL_LOG_GROUPING_MAX_TIME_DELTA 60000

/* 此结构定义 ACL 日志中的一个条目。 */
typedef struct ACLLogEntry {
    uint64_t count;     /* 最近发生此事件的次数 */
    int reason;         /* 拒绝命令的原因。ACL_DENIED_* */
    int context;        /* 顶层、Lua 还是 MULTI/EXEC？ACL_LOG_CTX_* */
    sds object;         /* 键名或命令名 */
    sds username;       /* 客户端认证的用户 */
    mstime_t ctime;     /* 此条目最后一次更新的毫秒时间 */
    sds cinfo;          /* 客户端信息（更新后的最后一个客户端） */
    long long entry_id;         /* (entry_id, timestamp_created) 对是此条目的唯一标识符，
                                  * 在节点崩溃重启时可检测是否为新系列。 */
    mstime_t timestamp_created; /* 此条目创建时的 UNIX 毫秒时间 */
} ACLLogEntry;

/* 此函数检查 ACL 条目 'a' 和 'b' 是否足够相似，
 * 以便更新现有 ACL 日志条目而不是创建新条目。 */
int ACLLogMatchEntry(ACLLogEntry *a, ACLLogEntry *b) {
    if (a->reason != b->reason) return 0;
    if (a->context != b->context) return 0;
    mstime_t delta = a->ctime - b->ctime;
    if (delta < 0) delta = -delta;
    if (delta > ACL_LOG_GROUPING_MAX_TIME_DELTA) return 0;
    if (sdscmp(a->object,b->object) != 0) return 0;
    if (sdscmp(a->username,b->username) != 0) return 0;
    return 1;
}

/* 释放 ACL 日志条目。 */
void ACLFreeLogEntry(void *leptr) {
    ACLLogEntry *le = leptr;
    sdsfree(le->object);
    sdsfree(le->username);
    sdsfree(le->cinfo);
    zfree(le);
}

/* 根据原因更新相关计数器 */
void ACLUpdateInfoMetrics(int reason){
    if (reason == ACL_DENIED_AUTH) {
        server.acl_info.user_auth_failures++;
    } else if (reason == ACL_DENIED_CMD) {
        server.acl_info.invalid_cmd_accesses++;
    } else if (reason == ACL_DENIED_KEY) {
        server.acl_info.invalid_key_accesses++;
    } else if (reason == ACL_DENIED_CHANNEL) {
        server.acl_info.invalid_channel_accesses++;
    } else {
        serverPanic("Unknown ACL_DENIED encoding");
    }
}

static void trimACLLogEntriesToMaxLen(void) {
    while(listLength(ACLLog) > server.acllog_max_len) {
        listNode *ln = listLast(ACLLog);
        ACLLogEntry *le = listNodeValue(ln);
        ACLFreeLogEntry(le);
        listDelNode(ACLLog,ln);
    }
}

/* 向 ACL 日志添加新条目，确保在达到日志最大长度时删除旧条目。
 * 此函数尝试在当前日志中查找相似条目，以递增日志条目的计数器，
 * 而不是为非常相似的 ACL 规则问题创建多个条目。
 *
 * 当原因是 ACL_DENIED_KEY 或 ACL_DENIED_CHANNEL 时使用 argpos 参数，
 * 因为它允许函数记录导致问题的键或频道名称。
 *
 * 最后 2 个参数是手动覆盖，替代依赖客户端和原因参数的自动值
 * （使用 NULL 表示默认值）。
 *
 * 如果 `object` 不为 NULL，此函数接管其所有权。
 */
void addACLLogEntry(client *c, int reason, int context, int argpos, sds username, sds object) {
    /* 更新 ACL 信息指标 */
    ACLUpdateInfoMetrics(reason);
    
    if (server.acllog_max_len == 0) {
        trimACLLogEntriesToMaxLen();
        return;
    }
    
    /* 创建新条目 */
    struct ACLLogEntry *le = zmalloc(sizeof(*le));
    le->count = 1;
    le->reason = reason;
    le->username = sdsdup(username ? username : c->user->name);
    le->ctime = commandTimeSnapshot();
    le->entry_id = ACLLogEntryCount;
    le->timestamp_created = le->ctime;

    if (object) {
        le->object = object;
    } else {
        switch(reason) {
            case ACL_DENIED_CMD: le->object = sdsdup(c->cmd->fullname); break;
            case ACL_DENIED_KEY: le->object = sdsdup(c->argv[argpos]->ptr); break;
            case ACL_DENIED_CHANNEL: le->object = sdsdup(c->argv[argpos]->ptr); break;
            case ACL_DENIED_AUTH: le->object = sdsdup(c->argv[0]->ptr); break;
            default: le->object = sdsempty();
        }
    }

    /* 如果有来自网络的真实客户端，使用它（模块定时器中可能缺失） */
    client *realclient = server.current_client? server.current_client : c;

    le->cinfo = catClientInfoString(sdsempty(),realclient);
    le->context = context;

    /* 尝试将此条目与过去的条目匹配，看是否可以
     * 更新现有条目而不是创建新条目。 */
    long toscan = 10; /* 有限地尝试查找重复项。 */
    listIter li;
    listNode *ln;
    listRewind(ACLLog,&li);
    ACLLogEntry *match = NULL;
    while (toscan-- && (ln = listNext(&li)) != NULL) {
        ACLLogEntry *current = listNodeValue(ln);
        if (ACLLogMatchEntry(current,le)) {
            match = current;
            listDelNode(ACLLog,ln);
            listAddNodeHead(ACLLog,current);
            break;
        }
    }

    /* 如果匹配则更新条目，否则作为新条目添加。 */
    if (match) {
        /* 更新现有条目的几个字段并递增此条目的事件计数器。 */
        sdsfree(match->cinfo);
        match->cinfo = le->cinfo;
        match->ctime = le->ctime;
        match->count++;

        /* 释放旧条目 */
        le->cinfo = NULL;
        ACLFreeLogEntry(le);
    } else {
        /* 将其添加到条目列表。需要将列表裁剪到最大大小。 */
        ACLLogEntryCount++; /* 递增 entry_id 计数以使日志中每条记录唯一。 */
        listAddNodeHead(ACLLog, le);
        trimACLLogEntriesToMaxLen();
    }
}

sds getAclErrorMessage(int acl_res, user *user, struct redisCommand *cmd, sds errored_val, int verbose) {
    switch (acl_res) {
    case ACL_DENIED_CMD:
        return sdscatfmt(sdsempty(), "User %S has no permissions to run "
                                     "the '%S' command", user->name, cmd->fullname);
    case ACL_DENIED_KEY:
        if (verbose) {
            return sdscatfmt(sdsempty(), "User %S has no permissions to access "
                                         "the '%S' key", user->name, errored_val);
        } else {
            return sdsnew("No permissions to access a key");
        }
    case ACL_DENIED_CHANNEL:
        if (verbose) {
            return sdscatfmt(sdsempty(), "User %S has no permissions to access "
                                         "the '%S' channel", user->name, errored_val);
        } else {
            return sdsnew("No permissions to access a channel");
        }
    }
    serverPanic("Reached deadcode on getAclErrorMessage");
}

/* =============================================================================
 * ACL 相关命令
 * ==========================================================================*/

/* ACL CAT 分类 */
void aclCatWithFlags(client *c, dict *commands, uint64_t cflag, int *arraylen) {
    dictEntry *de;
    dictIterator *di = dictGetIterator(commands);

    while ((de = dictNext(di)) != NULL) {
        struct redisCommand *cmd = dictGetVal(de);
        if (cmd->flags & CMD_MODULE) continue;
        if (cmd->acl_categories & cflag) {
            addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
            (*arraylen)++;
        }

        if (cmd->subcommands_dict) {
            aclCatWithFlags(c, cmd->subcommands_dict, cflag, arraylen);
        }
    }
    dictReleaseIterator(di);
}

/* 将单个选择器的格式化响应添加到 ACL GETUSER 响应中。
 * 此函数返回添加的字段数量。
 *
 * 设置 verbose 为 1 表示显示键和频道权限的完整限定符。
 */
int aclAddReplySelectorDescription(client *c, aclSelector *s) {
    listIter li;
    listNode *ln;

    /* 命令 */
    addReplyBulkCString(c,"commands");
    sds cmddescr = ACLDescribeSelectorCommandRules(s);
    addReplyBulkSds(c,cmddescr);
    
    /* 键模式 */
    addReplyBulkCString(c,"keys");
    if (s->flags & SELECTOR_FLAG_ALLKEYS) {
        addReplyBulkCBuffer(c,"~*",2);
    } else {
        sds dsl = sdsempty();
        listRewind(s->patterns,&li);
        while((ln = listNext(&li))) {
            keyPattern *thispat = (keyPattern *) listNodeValue(ln);
            if (ln != listFirst(s->patterns)) dsl = sdscat(dsl, " ");
            dsl = sdsCatPatternString(dsl, thispat);
        }
        addReplyBulkSds(c, dsl);
    }

    /* Pub/Sub 模式 */
    addReplyBulkCString(c,"channels");
    if (s->flags & SELECTOR_FLAG_ALLCHANNELS) {
        addReplyBulkCBuffer(c,"&*",2);
    } else {
        sds dsl = sdsempty();
        listRewind(s->channels,&li);
        while((ln = listNext(&li))) {
            sds thispat = listNodeValue(ln);
            if (ln != listFirst(s->channels)) dsl = sdscat(dsl, " ");
            dsl = sdscatfmt(dsl, "&%S", thispat);
        }
        addReplyBulkSds(c, dsl);
    }
    return 3;
}

/* ACL -- 显示和修改 ACL 用户的配置。
 * ACL HELP
 * ACL LOAD
 * ACL SAVE
 * ACL LIST
 * ACL USERS
 * ACL CAT [<category>]
 * ACL SETUSER <username> ... acl rules ...
 * ACL DELUSER <username> [...]
 * ACL GETUSER <username>
 * ACL GENPASS [<bits>]
 * ACL WHOAMI
 * ACL LOG [<count> | RESET]
 */
void aclCommand(client *c) {
    char *sub = c->argv[1]->ptr;
    if (!strcasecmp(sub,"setuser") && c->argc >= 3) {
        /* 首先脱敏所有参数，以不泄露任何关于用户的信息。 */
        for (int j = 2; j < c->argc; j++) {
            redactClientCommandArgument(c, j);
        }

        sds username = c->argv[2]->ptr;
        /* 检查用户名有效性 */
        if (ACLStringHasSpaces(username,sdslen(username))) {
            addReplyError(c, "Usernames can't contain spaces or null characters");
            return;
        }

        user *u = ACLGetUserByName(username,sdslen(username));

        sds *temp_argv = zmalloc(c->argc * sizeof(sds));
        for (int i = 3; i < c->argc; i++) temp_argv[i-3] = c->argv[i]->ptr;

        sds error = ACLStringSetUser(u, username, temp_argv, c->argc - 3);
        zfree(temp_argv);
        if (error == NULL) {
            addReply(c,shared.ok);
        } else {
            addReplyErrorSdsSafe(c, error);
        }
        return;
    } else if (!strcasecmp(sub,"deluser") && c->argc >= 3) {
        /* 首先脱敏所有参数，以不泄露任何关于用户的信息。 */
        for (int j = 2; j < c->argc; j++) redactClientCommandArgument(c, j);

        int deleted = 0;
        for (int j = 2; j < c->argc; j++) {
            sds username = c->argv[j]->ptr;
            if (!strcmp(username,"default")) {
                addReplyError(c,"The 'default' user cannot be removed");
                return;
            }
        }

        for (int j = 2; j < c->argc; j++) {
            sds username = c->argv[j]->ptr;
            user *u;
            if (raxRemove(Users,(unsigned char*)username,
                          sdslen(username),
                          (void**)&u))
            {
                ACLFreeUserAndKillClients(u);
                deleted++;
            }
        }
        addReplyLongLong(c,deleted);
    } else if (!strcasecmp(sub,"getuser") && c->argc == 3) {
        /* 脱敏用户名以不泄露任何关于用户的信息。 */
        redactClientCommandArgument(c, 2);

        user *u = ACLGetUserByName(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
        if (u == NULL) {
            addReplyNull(c);
            return;
        }

        void *ufields = addReplyDeferredLen(c);
        int fields = 3;

        /* 标志 */
        addReplyBulkCString(c,"flags");
        void *deflen = addReplyDeferredLen(c);
        int numflags = 0;
        for (int j = 0; ACLUserFlags[j].flag; j++) {
            if (u->flags & ACLUserFlags[j].flag) {
                addReplyBulkCString(c,ACLUserFlags[j].name);
                numflags++;
            }
        }
        setDeferredSetLen(c,deflen,numflags);

        /* 密码 */
        addReplyBulkCString(c,"passwords");
        addReplyArrayLen(c,listLength(u->passwords));
        listIter li;
        listNode *ln;
        listRewind(u->passwords,&li);
        while((ln = listNext(&li))) {
            sds thispass = listNodeValue(ln);
            addReplyBulkCBuffer(c,thispass,sdslen(thispass));
        }
        /* 为向后兼容，在顶层包含根选择器 */
        fields += aclAddReplySelectorDescription(c, ACLUserGetRootSelector(u));

        /* 描述此用户的所有选择器，包括复制根选择器 */
        addReplyBulkCString(c,"selectors");
        addReplyArrayLen(c, listLength(u->selectors) - 1);
        listRewind(u->selectors,&li);
        serverAssert(listNext(&li));
        while((ln = listNext(&li))) {
            void *slen = addReplyDeferredLen(c);
            int sfields = aclAddReplySelectorDescription(c, (aclSelector *)listNodeValue(ln));
            setDeferredMapLen(c, slen, sfields);
        } 
        setDeferredMapLen(c, ufields, fields);
    } else if ((!strcasecmp(sub,"list") || !strcasecmp(sub,"users")) &&
               c->argc == 2)
    {
        int justnames = !strcasecmp(sub,"users");
        addReplyArrayLen(c,raxSize(Users));
        raxIterator ri;
        raxStart(&ri,Users);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            user *u = ri.data;
            if (justnames) {
                addReplyBulkCBuffer(c,u->name,sdslen(u->name));
            } else {
                /* 以配置文件格式返回信息。 */
                sds config = sdsnew("user ");
                config = sdscatsds(config,u->name);
                config = sdscatlen(config," ",1);
                robj *descr = ACLDescribeUser(u);
                config = sdscatsds(config,descr->ptr);
                decrRefCount(descr);
                addReplyBulkSds(c,config);
            }
        }
        raxStop(&ri);
    } else if (!strcasecmp(sub,"whoami") && c->argc == 2) {
        if (c->user != NULL) {
            addReplyBulkCBuffer(c,c->user->name,sdslen(c->user->name));
        } else {
            addReplyNull(c);
        }
    } else if (server.acl_filename[0] == '\0' &&
               (!strcasecmp(sub,"load") || !strcasecmp(sub,"save")))
    {
        addReplyError(c,"This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.");
        return;
    } else if (!strcasecmp(sub,"load") && c->argc == 2) {
        sds errors = ACLLoadFromFile(server.acl_filename);
        if (errors == NULL) {
            addReply(c,shared.ok);
        } else {
            addReplyError(c,errors);
            sdsfree(errors);
        }
    } else if (!strcasecmp(sub,"save") && c->argc == 2) {
        if (ACLSaveToFile(server.acl_filename) == C_OK) {
            addReply(c,shared.ok);
        } else {
            addReplyError(c,"There was an error trying to save the ACLs. "
                            "Please check the server logs for more "
                            "information");
        }
    } else if (!strcasecmp(sub,"cat") && c->argc == 2) {
        void *dl = addReplyDeferredLen(c);
        int j;
        for (j = 0; ACLCommandCategories[j].flag != 0; j++)
            addReplyBulkCString(c,ACLCommandCategories[j].name);
        setDeferredArrayLen(c,dl,j);
    } else if (!strcasecmp(sub,"cat") && c->argc == 3) {
        uint64_t cflag = ACLGetCommandCategoryFlagByName(c->argv[2]->ptr);
        if (cflag == 0) {
            addReplyErrorFormat(c, "Unknown category '%.128s'", (char*)c->argv[2]->ptr);
            return;
        }
        int arraylen = 0;
        void *dl = addReplyDeferredLen(c);
        aclCatWithFlags(c, server.orig_commands, cflag, &arraylen);
        setDeferredArrayLen(c,dl,arraylen);
    } else if (!strcasecmp(sub,"genpass") && (c->argc == 2 || c->argc == 3)) {
        #define GENPASS_MAX_BITS 4096
        char pass[GENPASS_MAX_BITS/8*2]; /* 十六进制表示 */
        long bits = 256; /* 默认生成 256 位密码 */

        if (c->argc == 3 && getLongFromObjectOrReply(c,c->argv[2],&bits,NULL)
            != C_OK) return;

        if (bits <= 0 || bits > GENPASS_MAX_BITS) {
            addReplyErrorFormat(c,
                "ACL GENPASS argument must be the number of "
                "bits for the output password, a positive number "
                "up to %d",GENPASS_MAX_BITS);
            return;
        }

        long chars = (bits+3)/4; /* 四舍五入到要输出的字符数 */
        getRandomHexChars(pass,chars);
        addReplyBulkCBuffer(c,pass,chars);
    } else if (!strcasecmp(sub,"log") && (c->argc == 2 || c->argc ==3)) {
        long count = 10; /* 默认输出的条目数量 */

        /* 解析 LOG 可能有的唯一参数：可以是用户想要显示的条目数量，
         * 或者 "RESET" 命令以清空旧条目。 */
        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr,"reset")) {
                listSetFreeMethod(ACLLog,ACLFreeLogEntry);
                listEmpty(ACLLog);
                listSetFreeMethod(ACLLog,NULL);
                addReply(c,shared.ok);
                return;
            } else if (getLongFromObjectOrReply(c,c->argv[2],&count,NULL)
                       != C_OK)
            {
                return;
            }
            if (count < 0) count = 0;
        }

        /* 根据获取的条目数量修正计数 */
        if ((size_t)count > listLength(ACLLog))
            count = listLength(ACLLog);

        addReplyArrayLen(c,count);
        listIter li;
        listNode *ln;
        listRewind(ACLLog,&li);
        mstime_t now = commandTimeSnapshot();
        while (count-- && (ln = listNext(&li)) != NULL) {
            ACLLogEntry *le = listNodeValue(ln);
            addReplyMapLen(c,10);
            addReplyBulkCString(c,"count");
            addReplyLongLong(c,le->count);

            addReplyBulkCString(c,"reason");
            char *reasonstr;
            switch(le->reason) {
            case ACL_DENIED_CMD: reasonstr="command"; break;
            case ACL_DENIED_KEY: reasonstr="key"; break;
            case ACL_DENIED_CHANNEL: reasonstr="channel"; break;
            case ACL_DENIED_AUTH: reasonstr="auth"; break;
            default: reasonstr="unknown";
            }
            addReplyBulkCString(c,reasonstr);

            addReplyBulkCString(c,"context");
            char *ctxstr;
            switch(le->context) {
            case ACL_LOG_CTX_TOPLEVEL: ctxstr="toplevel"; break;
            case ACL_LOG_CTX_MULTI: ctxstr="multi"; break;
            case ACL_LOG_CTX_LUA: ctxstr="lua"; break;
            case ACL_LOG_CTX_MODULE: ctxstr="module"; break;
            default: ctxstr="unknown";
            }
            addReplyBulkCString(c,ctxstr);

            addReplyBulkCString(c,"object");
            addReplyBulkCBuffer(c,le->object,sdslen(le->object));
            addReplyBulkCString(c,"username");
            addReplyBulkCBuffer(c,le->username,sdslen(le->username));
            addReplyBulkCString(c,"age-seconds");
            double age = (double)(now - le->ctime)/1000;
            addReplyDouble(c,age);
            addReplyBulkCString(c,"client-info");
            addReplyBulkCBuffer(c,le->cinfo,sdslen(le->cinfo));
            addReplyBulkCString(c, "entry-id");
            addReplyLongLong(c, le->entry_id);
            addReplyBulkCString(c, "timestamp-created");
            addReplyLongLong(c, le->timestamp_created);
            addReplyBulkCString(c, "timestamp-last-updated");
            addReplyLongLong(c, le->ctime);
        }
    } else if (!strcasecmp(sub,"dryrun") && c->argc >= 4) {
        struct redisCommand *cmd;
        user *u = ACLGetUserByName(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
        if (u == NULL) {
            addReplyErrorFormat(c, "User '%s' not found", (char *)c->argv[2]->ptr);
            return;
        }

        if ((cmd = lookupCommand(c->argv + 3, c->argc - 3)) == NULL) {
            addReplyErrorFormat(c, "Command '%s' not found", (char *)c->argv[3]->ptr);
            return;
        }

        if ((cmd->arity > 0 && cmd->arity != c->argc-3) ||
            (c->argc-3 < -cmd->arity))
        {
            addReplyErrorFormat(c,"wrong number of arguments for '%s' command", cmd->fullname);
            return;
        }

        int idx;
        int result = ACLCheckAllUserCommandPerm(u, cmd, c->argv + 3, c->argc - 3, &idx);
        if (result != ACL_OK) {
            sds err = getAclErrorMessage(result, u, cmd,  c->argv[idx+3]->ptr, 1);
            addReplyBulkSds(c, err);
            return;
        }

        addReply(c,shared.ok);
    } else if (c->argc == 2 && !strcasecmp(sub,"help")) {
        const char *help[] = {
"CAT [<category>]",
"    List all commands that belong to <category>, or all command categories",
"    when no category is specified.",
"DELUSER <username> [<username> ...]",
"    Delete a list of users.",
"DRYRUN <username> <command> [<arg> ...]",
"    Returns whether the user can execute the given command without executing the command.",
"GETUSER <username>",
"    Get the user's details.",
"GENPASS [<bits>]",
"    Generate a secure 256-bit user password. The optional `bits` argument can",
"    be used to specify a different size.",
"LIST",
"    Show users details in config file format.",
"LOAD",
"    Reload users from the ACL file.",
"LOG [<count> | RESET]",
"    Show the ACL log entries.",
"SAVE",
"    Save the current config to the ACL file.",
"SETUSER <username> <attribute> [<attribute> ...]",
"    Create or modify a user with the specified attributes.",
"USERS",
"    List all the registered usernames.",
"WHOAMI",
"    Return the current connection username.",
NULL
        };
        addReplyHelp(c,help);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

void addReplyCommandCategories(client *c, struct redisCommand *cmd) {
    int flagcount = 0;
    void *flaglen = addReplyDeferredLen(c);
    for (int j = 0; ACLCommandCategories[j].flag != 0; j++) {
        if (cmd->acl_categories & ACLCommandCategories[j].flag) {
            addReplyStatusFormat(c, "@%s", ACLCommandCategories[j].name);
            flagcount++;
        }
    }
    setDeferredSetLen(c, flaglen, flagcount);
}

/* AUTH <password>
 * AUTH <username> <password>（Redis >= 6.0 形式）
 *
 * 省略用户时意味着我们尝试以默认用户进行认证。 */
void authCommand(client *c) {
    /* 只允许两个或三个参数形式。 */
    if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    /* 始终脱敏第二个参数 */
    redactClientCommandArgument(c, 1);

    /* 处理两种不同的形式。两个参数的形式将使用 "default" 作为用户名。 */
    robj *username, *password;
    if (c->argc == 2) {
        /* 模拟旧行为：如果未配置密码，对两个参数形式给出错误。 */
        if (DefaultUser->flags & USER_FLAG_NOPASS) {
            addReplyError(c,"AUTH <password> called without any password "
                            "configured for the default user. Are you sure "
                            "your configuration is correct?");
            return;
        }

        username = shared.default_username; 
        password = c->argv[1];
    } else {
        username = c->argv[1];
        password = c->argv[2];
        redactClientCommandArgument(c, 2);
    }

    robj *err = NULL;
    int result = ACLAuthenticateUser(c, username, password, &err);
    if (result == AUTH_OK) {
        addReply(c, shared.ok);
    } else if (result == AUTH_ERR) {
        addAuthErrReply(c, err);
    }
    if (err) decrRefCount(err);
}

/* 设置 "default" ACL 用户的密码。这实现了 requirepass 配置的支持，
 * 传入 NULL 将设置用户为 nopass。 */
void ACLUpdateDefaultUserPassword(sds password) {
    ACLSetUser(DefaultUser,"resetpass",-1);
    if (password) {
        sds aclop = sdscatlen(sdsnew(">"), password, sdslen(password));
        ACLSetUser(DefaultUser,aclop,sdslen(aclop));
        sdsfree(aclop);
    } else {
        ACLSetUser(DefaultUser,"nopass",-1);
    }
}
