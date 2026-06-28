/* CLI（命令行接口）公共函数
 *
 * 本文件包含 Redis CLI 工具的通用功能函数，包括：
 * - TLS/SSL 安全连接配置
 * - URI 连接信息解析
 * - JSON 字符串转义
 * - 版本信息获取
 *
 * Copyright (c) 2020-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "fmacros.h"
#include "cli_common.h"
#include "version.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <hiredis.h>
/* 使用 hiredis 的 sds 兼容头文件，将 sds 调用映射到 hi_ 变体 */
#include <sdscompat.h>
/* 使用 hiredis 的 sds.h，使二进制文件中只有一组 sds 函数 */
#include <sds.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>
#endif

#define UNUSED(V) ((void) V)

char *redisGitSHA1(void);
char *redisGitDirty(void);

/* redisSecureConnection 的包装函数
 *
 * 在未启用 TLS 支持编译时，避免对 hiredis_ssl 的依赖 */
int cliSecureConnection(redisContext *c, cliSSLconfig config, const char **err) {
#ifdef USE_OPENSSL
    static SSL_CTX *ssl_ctx = NULL;

    if (!ssl_ctx) {
        ssl_ctx = SSL_CTX_new(SSLv23_client_method());
        if (!ssl_ctx) {
            *err = "Failed to create SSL_CTX";
            goto error;
        }
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
        SSL_CTX_set_verify(ssl_ctx, config.skip_cert_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);

        /* 加载 CA 证书或使用默认路径 */
        if (config.cacert || config.cacertdir) {
            if (!SSL_CTX_load_verify_locations(ssl_ctx, config.cacert, config.cacertdir)) {
                *err = "Invalid CA Certificate File/Directory";
                goto error;
            }
        } else {
            if (!SSL_CTX_set_default_verify_paths(ssl_ctx)) {
                *err = "Failed to use default CA paths";
                goto error;
            }
        }

        /* 加载客户端证书 */
        if (config.cert && !SSL_CTX_use_certificate_chain_file(ssl_ctx, config.cert)) {
            *err = "Invalid client certificate";
            goto error;
        }

        /* 加载客户端私钥 */
        if (config.key && !SSL_CTX_use_PrivateKey_file(ssl_ctx, config.key, SSL_FILETYPE_PEM)) {
            *err = "Invalid private key";
            goto error;
        }
        /* 设置加密套件 */
        if (config.ciphers && !SSL_CTX_set_cipher_list(ssl_ctx, config.ciphers)) {
            *err = "Error while configuring ciphers";
            goto error;
        }
#ifdef TLS1_3_VERSION
        /* 设置 TLS 1.3 加密套件 */
        if (config.ciphersuites && !SSL_CTX_set_ciphersuites(ssl_ctx, config.ciphersuites)) {
            *err = "Error while setting cypher suites";
            goto error;
        }
#endif
    }

    SSL *ssl = SSL_new(ssl_ctx);
    if (!ssl) {
        *err = "Failed to create SSL object";
        return REDIS_ERR;
    }

    /* 配置 SNI（Server Name Indication） */
    if (config.sni && !SSL_set_tlsext_host_name(ssl, config.sni)) {
        *err = "Failed to configure SNI";
        SSL_free(ssl);
        return REDIS_ERR;
    }

    return redisInitiateSSL(c, ssl);

error:
    SSL_CTX_free(ssl_ctx);
    ssl_ctx = NULL;
    return REDIS_ERR;
#else
    (void) config;
    (void) c;
    (void) err;
    return REDIS_OK;
#endif
}

/* 基于 hiredis 的包装函数，支持任意读写操作
 *
 * 我们在 hiredis 之上实现透明 TLS 支持，并使用其内部缓冲区，
 * 以便与之前/之后在连接上发出的命令共存。
 *
 * 接口与 read()/write() 足够接近，大部分操作应该能透明工作。 */

/* 通过 redisContext 写入原始缓冲区
 *
 * 若缓冲区中已有数据（来自 hiredis 操作的残留），也会一并写入 */
ssize_t cliWriteConn(redisContext *c, const char *buf, size_t buf_len)
{
    int done = 0;

    /* 将数据追加到缓冲区（通常应为空，但不假设），然后写入 */
    c->obuf = sdscatlen(c->obuf, buf, buf_len);
    if (redisBufferWrite(c, &done) == REDIS_ERR) {
        if (!(c->flags & REDIS_BLOCK))
            errno = EAGAIN;

        /* 发生错误时，假设未写入任何数据，将缓冲区回滚到原始状态 */
        if (sdslen(c->obuf) > buf_len)
            sdsrange(c->obuf, 0, -(buf_len+1));
        else
            sdsclear(c->obuf);

        return -1;
    }

    /* 若写入完成，释放所有内容。可能已写入超过 buf_len 的数据
     *（若 c->obuf 最初不为空），但我们不需要报告这一点 */
    if (done) {
        sdsclear(c->obuf);
        return buf_len;
    }

    /* 写入成功但有残留数据需要从缓冲区移除
     *
     * 检查是否仍有在我们的 buf 之前存在的数据？
     * 若是，恢复缓冲区到原始状态并报告未写入新数据 */
    if (sdslen(c->obuf) > buf_len) {
        sdsrange(c->obuf, 0, -(buf_len+1));
        return 0;
    }

    /* 此时确信没有之前的数据残留。刷新缓冲区并报告写入的数据量 */
    size_t left = sdslen(c->obuf);
    sdsclear(c->obuf);
    return buf_len - left;
}

/* OpenSSL（libssl 和 libcrypto）初始化包装函数 */
int cliSecureInit(void)
{
#ifdef USE_OPENSSL
    ERR_load_crypto_strings();
    SSL_load_error_strings();
    SSL_library_init();
#endif
    return REDIS_OK;
}

/* 从标准输入创建 sds 字符串 */
sds readArgFromStdin(void) {
    char buf[1024];
    sds arg = sdsempty();

    while(1) {
        int nread = read(fileno(stdin),buf,1024);

        if (nread == 0) break;
        else if (nread == -1) {
            perror("Reading from standard input");
            exit(1);
        }
        arg = sdscatlen(arg,buf,nread);
    }
    return arg;
}

/* 从 argv 创建 sds 数组
 *
 * 可以原样返回，也可以对每个元素进行去引号处理。
 * 当 quoted 非零时，若遇到无效的引号字符串则返回 NULL。
 *
 * 调用者应使用 sdsfreesplitres() 释放返回的 sds 字符串数组。 */
sds *getSdsArrayFromArgv(int argc,char **argv, int quoted) {
    sds *res = sds_malloc(sizeof(sds) * argc);

    for (int j = 0; j < argc; j++) {
        if (quoted) {
            sds unquoted = unquoteCString(argv[j]);
            if (!unquoted) {
                while (--j >= 0) sdsfree(res[j]);
                sds_free(res);
                return NULL;
            }
            res[j] = unquoted;
        } else {
            res[j] = sdsnew(argv[j]);
        }
    }

    return res;
}

/* 对以 null 结尾的字符串进行去引号处理，返回二进制安全的 sds */
sds unquoteCString(char *str) {
    int count;
    sds *unquoted = sdssplitargs(str, &count);
    sds res = NULL;

    if (unquoted && count == 1) {
        res = unquoted[0];
        unquoted[0] = NULL;
    }

    if (unquoted)
        sdsfreesplitres(unquoted, count);

    return res;
}


/* URL 风格的百分号解码（Percent decoding） */
#define isHexChar(c) (isdigit(c) || ((c) >= 'a' && (c) <= 'f'))
#define decodeHexChar(c) (isdigit(c) ? (c) - '0' : (c) - 'a' + 10)
#define decodeHex(h, l) ((decodeHexChar(h) << 4) + decodeHexChar(l))

/* 百分号解码实现
 *
 * 将 %XX 格式的编码字符串解码为原始字节 */
static sds percentDecode(const char *pe, size_t len) {
    const char *end = pe + len;
    sds ret = sdsempty();
    const char *curr = pe;

    while (curr < end) {
        if (*curr == '%') {
            if ((end - curr) < 2) {
                fprintf(stderr, "Incomplete URI encoding\n");
                exit(1);
            }

            char h = tolower(*(++curr));
            char l = tolower(*(++curr));
            if (!isHexChar(h) || !isHexChar(l)) {
                fprintf(stderr, "Illegal character in URI encoding\n");
                exit(1);
            }
            char c = decodeHex(h, l);
            ret = sdscatlen(ret, &c, 1);
            curr++;
        } else {
            ret = sdscatlen(ret, curr++, 1);
        }
    }

    return ret;
}

/* 解析 URI 并提取服务器连接信息
 *
 * URI 格式基于临时规范[1]，不支持查询参数。有效 URI 格式：
 *   scheme:    "redis://"
 *   authority: [[<username> ":"] <password> "@"] [<hostname> [":" <port>]]
 *   path:      ["/" [<db>]]
 *
 *  [1]: https://www.iana.org/assignments/uri-schemes/prov/redis */
void parseRedisUri(const char *uri, const char* tool_name, cliConnInfo *connInfo, int *tls_flag) {
#ifdef USE_OPENSSL
    UNUSED(tool_name);
#else
    UNUSED(tls_flag);
#endif

    const char *scheme = "redis://";
    const char *tlsscheme = "rediss://";
    const char *curr = uri;
    const char *end = uri + strlen(uri);
    const char *userinfo, *username, *port, *host, *path;

    /* URI 必须以有效 scheme 开头 */
    if (!strncasecmp(tlsscheme, curr, strlen(tlsscheme))) {
#ifdef USE_OPENSSL
        *tls_flag = 1;
        curr += strlen(tlsscheme);
#else
        fprintf(stderr,"rediss:// is only supported when %s is compiled with OpenSSL\n", tool_name);
        exit(1);
#endif
    } else if (!strncasecmp(scheme, curr, strlen(scheme))) {
        curr += strlen(scheme);
    } else {
        fprintf(stderr,"Invalid URI scheme\n");
        exit(1);
    }
    if (curr == end) return;

    /* 提取用户信息（username:password） */
    if ((userinfo = strchr(curr,'@'))) {
        if ((username = strchr(curr, ':')) && username < userinfo) {
            connInfo->user = percentDecode(curr, username - curr);
            curr = username + 1;
        }

        connInfo->auth = percentDecode(curr, userinfo - curr);
        curr = userinfo + 1;
    }
    if (curr == end) return;

    /* 提取主机和端口 */
    path = strchr(curr, '/');
    if (*curr != '/') {
        host = path ? path - 1 : end;
        if (*curr == '[') {
            /* IPv6 地址格式 [...] */
            curr += 1;
            if ((port = strchr(curr, ']'))) {
                if (*(port+1) == ':') {
                    connInfo->hostport = atoi(port + 2);
                }
                host = port - 1;
            }
        } else {
            if ((port = strchr(curr, ':'))) {
                connInfo->hostport = atoi(port + 1);
                host = port - 1;
            }
        }
        sdsfree(connInfo->hostip);
        connInfo->hostip = sdsnewlen(curr, host - curr + 1);
    }
    curr = path ? path + 1 : end;
    if (curr == end) return;

    /* 提取数据库编号 */
    connInfo->input_dbnum = atoi(curr);
}

/* 释放 cliConnInfo 结构体中的动态分配内存 */
void freeCliConnInfo(cliConnInfo connInfo){
    if (connInfo.hostip) sdsfree(connInfo.hostip);
    if (connInfo.auth) sdsfree(connInfo.auth);
    if (connInfo.user) sdsfree(connInfo.user);
}

/* JSON 输出字符串转义
 *
 * 按照 RFC 7159 规范对 Unicode 字符串进行转义：
 * https://datatracker.ietf.org/doc/html/rfc7159#section-7 */
sds escapeJsonString(sds s, const char *p, size_t len) {
    s = sdscatlen(s,"\"",1);
    while(len--) {
        switch(*p) {
        case '\\':
        case '"':
            s = sdscatprintf(s,"\\%c",*p);
            break;
        case '\n': s = sdscatlen(s,"\\n",2); break;
        case '\f': s = sdscatlen(s,"\\f",2); break;
        case '\r': s = sdscatlen(s,"\\r",2); break;
        case '\t': s = sdscatlen(s,"\\t",2); break;
        case '\b': s = sdscatlen(s,"\\b",2); break;
        default:
            s = sdscatprintf(s,*(unsigned char *)p <= 0x1f ? "\\u%04x" : "%c",*p);
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}

/* 获取 CLI 版本字符串
 *
 * 包含 Redis 版本、Git commit SHA1 以及工作区状态（若可用） */
sds cliVersion(void) {
    sds version = sdscatprintf(sdsempty(), "%s", REDIS_VERSION);

    /* 当 Git 信息可用时，添加 Git commit 和工作区状态 */
    if (strtoll(redisGitSHA1(),NULL,16)) {
        version = sdscatprintf(version, " (git:%s", redisGitSHA1());
        if (strtoll(redisGitDirty(),NULL,10))
            version = sdscatprintf(version, "-dirty");
        version = sdscat(version, ")");
    }
    return version;
}

/* redisConnect 或 redisConnectWithTimeout 的包装函数
 *
 * 若超时时间为零则使用无超时版本 */
redisContext *redisConnectWrapper(const char *ip, int port, const struct timeval tv) {
    if (tv.tv_sec == 0 && tv.tv_usec == 0) {
        return redisConnect(ip, port);
    } else {
        return redisConnectWithTimeout(ip, port, tv);
    }
}

/* redisConnectUnix 或 redisConnectUnixWithTimeout 的包装函数
 *
 * 若超时时间为零则使用无超时版本 */
redisContext *redisConnectUnixWrapper(const char *path, const struct timeval tv) {
    if (tv.tv_sec == 0 && tv.tv_usec == 0) {
        return redisConnectUnix(path);
    } else {
        return redisConnectUnixWithTimeout(path, tv);
    }
}
