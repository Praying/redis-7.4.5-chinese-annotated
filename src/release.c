/* Redis 版本信息与构建 ID
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

/* 此文件在 Redis Git SHA1 或 Dirty 状态变化时会被重新编译，
 * 因为其他文件通过这些函数访问版本信息。 */

#include <string.h>
#include <stdio.h>

#include "release.h"
#include "crc64.h"

/* 返回 Redis Git commit SHA1 标识 */
char *redisGitSHA1(void) {
    return REDIS_GIT_SHA1;
}

/* 返回 Git 工作区 dirty 状态标识 */
char *redisGitDirty(void) {
    return REDIS_GIT_DIRTY;
}

/* 返回原始构建 ID 字符串 */
const char *redisBuildIdRaw(void) {
    return REDIS_BUILD_ID_RAW;
}

/* 计算并返回构建 ID 的 CRC64 校验值 */
uint64_t redisBuildId(void) {
    char *buildid = REDIS_BUILD_ID_RAW;

    return crc64(0,(unsigned char*)buildid,strlen(buildid));
}

/* 返回构建 ID 的十六进制字符串（带缓存）
 *
 * 为避免每次都将构建 ID 转换为十六进制字符串（此操作在 INFO 输出中频繁调用），
 * 这里使用静态变量缓存结果。 */
char *redisBuildIdString(void) {
    static char buf[32];
    static int cached = 0;
    if (!cached) {
        snprintf(buf,sizeof(buf),"%llx",(unsigned long long) redisBuildId());
        cached = 1;
    }
    return buf;
}
