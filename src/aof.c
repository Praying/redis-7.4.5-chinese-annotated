/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "bio.h"
#include "rio.h"
#include "functions.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>

void freeClientArgv(client *c);
off_t getAppendOnlyFileSize(sds filename, int *status);
off_t getBaseAndIncrAppendOnlyFilesSize(aofManifest *am, int *status);
int getBaseAndIncrAppendOnlyFilesNum(aofManifest *am);
int aofFileExist(char *filename);
int rewriteAppendOnlyFile(char *filename);
aofManifest *aofLoadManifestFromFile(sds am_filepath);
void aofManifestFreeAndUpdate(aofManifest *am);
void aof_background_fsync_and_close(int fd);

/* ----------------------------------------------------------------------------
 * AOF Manifest 文件实现。
 *
 * 以下代码实现了 AOF manifest 文件的读写逻辑，该文件用于跟踪和管理所有 AOF 文件。
 *
 * 追加式（AOF）文件由三种类型组成：
 *
 * BASE：表示最近一次 AOF 重写时的 Redis 快照。manifest 文件中最多包含一个 BASE 文件，
 * 它始终是列表中的第一个文件。
 *
 * INCR：表示最近一次成功 AOF 重写之后 Redis 执行的所有写命令。在某些情况下可能会有
 * 多个有序的 INCR 文件。例如：
 *   - 在 AOF 重写正在进行时
 *   - 在一次 AOF 重写被中止/失败后、下一次成功重写之前。
 *
 * HISTORY：在重写成功后，先前的 BASE 和 INCR 文件变为 HISTORY 类型。除非禁用垃圾回收，
 * 否则它们将被自动删除。
 *
 * 以下是可能的 AOF manifest 文件内容示例：
 *
 * file appendonly.aof.2.base.rdb seq 2 type b
 * file appendonly.aof.1.incr.aof seq 1 type h
 * file appendonly.aof.2.incr.aof seq 2 type h
 * file appendonly.aof.3.incr.aof seq 3 type h
 * file appendonly.aof.4.incr.aof seq 4 type i
 * file appendonly.aof.5.incr.aof seq 5 type i
 * ------------------------------------------------------------------------- */

/* 命名规则。 */
#define BASE_FILE_SUFFIX           ".base"
#define INCR_FILE_SUFFIX           ".incr"
#define RDB_FORMAT_SUFFIX          ".rdb"
#define AOF_FORMAT_SUFFIX          ".aof"
#define MANIFEST_NAME_SUFFIX       ".manifest"
#define TEMP_FILE_NAME_PREFIX      "temp-"

/* AOF manifest 关键字。 */
#define AOF_MANIFEST_KEY_FILE_NAME   "file"
#define AOF_MANIFEST_KEY_FILE_SEQ    "seq"
#define AOF_MANIFEST_KEY_FILE_TYPE   "type"

/* 创建一个空的 aofInfo。 */
aofInfo *aofInfoCreate(void) {
    return zcalloc(sizeof(aofInfo));
}

/* 释放 aofInfo 结构（由 ai 指向）及其内嵌的 file_name。 */
void aofInfoFree(aofInfo *ai) {
    serverAssert(ai != NULL);
    if (ai->file_name) sdsfree(ai->file_name);
    zfree(ai);
}

/* 深拷贝一个 aofInfo。 */
aofInfo *aofInfoDup(aofInfo *orig) {
    serverAssert(orig != NULL);
    aofInfo *ai = aofInfoCreate();
    ai->file_name = sdsdup(orig->file_name);
    ai->file_seq = orig->file_seq;
    ai->file_type = orig->file_type;
    return ai;
}

/* 将 aofInfo 格式化为字符串，作为 manifest 中的一行。
 *
 * 更新此格式时，请同时更新 redis-check-aof 工具。 */
sds aofInfoFormat(sds buf, aofInfo *ai) {
    sds filename_repr = NULL;

    if (sdsneedsrepr(ai->file_name))
        filename_repr = sdscatrepr(sdsempty(), ai->file_name, sdslen(ai->file_name));

    sds ret = sdscatprintf(buf, "%s %s %s %lld %s %c\n",
        AOF_MANIFEST_KEY_FILE_NAME, filename_repr ? filename_repr : ai->file_name,
        AOF_MANIFEST_KEY_FILE_SEQ, ai->file_seq,
        AOF_MANIFEST_KEY_FILE_TYPE, ai->file_type);
    sdsfree(filename_repr);

    return ret;
}

/* 释放 AOF 列表元素的方法。 */
void aofListFree(void *item) {
    aofInfo *ai = (aofInfo *)item;
    aofInfoFree(ai);
}

/* 复制 AOF 列表元素的方法。 */
void *aofListDup(void *item) {
    return aofInfoDup(item);
}

/* 创建一个空的 aofManifest，将在 `aofLoadManifestFromDisk` 中调用。 */
aofManifest *aofManifestCreate(void) {
    aofManifest *am = zcalloc(sizeof(aofManifest));
    am->incr_aof_list = listCreate();
    am->history_aof_list = listCreate();
    listSetFreeMethod(am->incr_aof_list, aofListFree);
    listSetDupMethod(am->incr_aof_list, aofListDup);
    listSetFreeMethod(am->history_aof_list, aofListFree);
    listSetDupMethod(am->history_aof_list, aofListDup);
    return am;
}

/* 释放 aofManifest 结构（由 am 指向）及其内嵌成员。 */
void aofManifestFree(aofManifest *am) {
    if (am->base_aof_info) aofInfoFree(am->base_aof_info);
    if (am->incr_aof_list) listRelease(am->incr_aof_list);
    if (am->history_aof_list) listRelease(am->history_aof_list);
    zfree(am);
}

/* 获取 AOF manifest 文件名。 */
sds getAofManifestFileName(void) {
    return sdscatprintf(sdsempty(), "%s%s", server.aof_filename,
                MANIFEST_NAME_SUFFIX);
}

/* 获取临时 AOF manifest 文件名。 */
sds getTempAofManifestFileName(void) {
    return sdscatprintf(sdsempty(), "%s%s%s", TEMP_FILE_NAME_PREFIX,
                server.aof_filename, MANIFEST_NAME_SUFFIX);
}

/* 返回由 am 指向的 aofManifest 的字符串表示形式。
 *
 * 该字符串是由 '\n' 分隔的多行，每行代表一个 AOF 文件。
 *
 * 每行以空格分隔，包含 6 个字段，格式如下：
 * "file" [filename] "seq" [sequence] "type" [type]
 *
 * 其中 "file"、"seq" 和 "type" 是描述下一个值的关键字，
 * [filename] 和 [sequence] 描述文件名和顺序，[type] 是
 * 'b'（base）、'h'（history）或 'i'（incr）之一。
 *
 * 如果存在 BASE 文件，它将始终排在最前面，接下来是 HISTORY 文件，
 * 最后是 INCR 文件。
 */
sds getAofManifestAsString(aofManifest *am) {
    serverAssert(am != NULL);

    sds buf = sdsempty();
    listNode *ln;
    listIter li;

    /* 1. 添加 BASE 文件信息，它始终位于 manifest 文件的开头。 */
    if (am->base_aof_info) {
        buf = aofInfoFormat(buf, am->base_aof_info);
    }

    /* 2. 添加 HISTORY 类型 AOF 信息。 */
    listRewind(am->history_aof_list, &li);
    while ((ln = listNext(&li)) != NULL) {
        aofInfo *ai = (aofInfo*)ln->value;
        buf = aofInfoFormat(buf, ai);
    }

    /* 3. 添加 INCR 类型 AOF 信息。 */
    listRewind(am->incr_aof_list, &li);
    while ((ln = listNext(&li)) != NULL) {
        aofInfo *ai = (aofInfo*)ln->value;
        buf = aofInfoFormat(buf, ai);
    }

    return buf;
}

/* 在 Redis 服务器启动时将 manifest 信息从磁盘加载到 `server.aof_manifest`。
 *
 * 在加载过程中，此函数执行严格的错误检查，并在出现错误（I/O 错误、格式无效等）
 * 时中止整个 Redis 服务器进程。
 *
 * 如果 AOF 目录或 manifest 文件不存在，则将其忽略，
 * 以支持从之前未使用它们的版本平滑升级。
 */
void aofLoadManifestFromDisk(void) {
    server.aof_manifest = aofManifestCreate();
    if (!dirExists(server.aof_dirname)) {
        serverLog(LL_DEBUG, "The AOF directory %s doesn't exist", server.aof_dirname);
        return;
    }

    sds am_name = getAofManifestFileName();
    sds am_filepath = makePath(server.aof_dirname, am_name);
    if (!fileExist(am_filepath)) {
        serverLog(LL_DEBUG, "The AOF manifest file %s doesn't exist", am_name);
        sdsfree(am_name);
        sdsfree(am_filepath);
        return;
    }

    aofManifest *am = aofLoadManifestFromFile(am_filepath);
    if (am) aofManifestFreeAndUpdate(am);
    sdsfree(am_name);
    sdsfree(am_filepath);
}

/* 通用的 manifest 加载函数，用于 `aofLoadManifestFromDisk` 和 redis-check-aof 工具。 */
#define MANIFEST_MAX_LINE 1024
aofManifest *aofLoadManifestFromFile(sds am_filepath) {
    const char *err = NULL;
    long long maxseq = 0;

    aofManifest *am = aofManifestCreate();
    FILE *fp = fopen(am_filepath, "r");
    if (fp == NULL) {
        serverLog(LL_WARNING, "Fatal error: can't open the AOF manifest "
            "file %s for reading: %s", am_filepath, strerror(errno));
        exit(1);
    }

    char buf[MANIFEST_MAX_LINE+1];
    sds *argv = NULL;
    int argc;
    aofInfo *ai = NULL;

    sds line = NULL;
    int linenum = 0;

    while (1) {
        if (fgets(buf, MANIFEST_MAX_LINE+1, fp) == NULL) {
            if (feof(fp)) {
                if (linenum == 0) {
                    err = "Found an empty AOF manifest";
                    goto loaderr;
                } else {
                    break;
                }
            } else {
                err = "Read AOF manifest failed";
                goto loaderr;
            }
        }

        linenum++;

        /* 跳过注释行 */
        if (buf[0] == '#') continue;

        if (strchr(buf, '\n') == NULL) {
            err = "The AOF manifest file contains too long line";
            goto loaderr;
        }

        line = sdstrim(sdsnew(buf), " \t\r\n");
        if (!sdslen(line)) {
            err = "Invalid AOF manifest file format";
            goto loaderr;
        }

        argv = sdssplitargs(line, &argc);
        /* 'argc < 6' 是为了前向兼容而做的检查。 */
        if (argv == NULL || argc < 6 || (argc % 2)) {
            err = "Invalid AOF manifest file format";
            goto loaderr;
        }

        ai = aofInfoCreate();
        for (int i = 0; i < argc; i += 2) {
            if (!strcasecmp(argv[i], AOF_MANIFEST_KEY_FILE_NAME)) {
                ai->file_name = sdsnew(argv[i+1]);
                if (!pathIsBaseName(ai->file_name)) {
                    err = "File can't be a path, just a filename";
                    goto loaderr;
                }
            } else if (!strcasecmp(argv[i], AOF_MANIFEST_KEY_FILE_SEQ)) {
                ai->file_seq = atoll(argv[i+1]);
            } else if (!strcasecmp(argv[i], AOF_MANIFEST_KEY_FILE_TYPE)) {
                ai->file_type = (argv[i+1])[0];
            }
            /* else if (!strcasecmp(argv[i], AOF_MANIFEST_KEY_OTHER)) {} */
        }

        /* 我们必须确保加载所有信息。 */
        if (!ai->file_name || !ai->file_seq || !ai->file_type) {
            err = "Invalid AOF manifest file format";
            goto loaderr;
        }

        sdsfreesplitres(argv, argc);
        argv = NULL;

        if (ai->file_type == AOF_FILE_TYPE_BASE) {
            if (am->base_aof_info) {
                err = "Found duplicate base file information";
                goto loaderr;
            }
            am->base_aof_info = ai;
            am->curr_base_file_seq = ai->file_seq;
        } else if (ai->file_type == AOF_FILE_TYPE_HIST) {
            listAddNodeTail(am->history_aof_list, ai);
        } else if (ai->file_type == AOF_FILE_TYPE_INCR) {
            if (ai->file_seq <= maxseq) {
                err = "Found a non-monotonic sequence number";
                goto loaderr;
            }
            listAddNodeTail(am->incr_aof_list, ai);
            am->curr_incr_file_seq = ai->file_seq;
            maxseq = ai->file_seq;
        } else {
            err = "Unknown AOF file type";
            goto loaderr;
        }

        sdsfree(line);
        line = NULL;
        ai = NULL;
    }

    fclose(fp);
    return am;

loaderr:
    /* Sanitizer 抑制：如果我们跳转到 loaderr 并在不释放这些分配的情况下 exit(1)，
     * 可能会误报。 */
    if (argv) sdsfreesplitres(argv, argc);
    if (ai) aofInfoFree(ai);

    serverLog(LL_WARNING, "\n*** FATAL AOF MANIFEST FILE ERROR ***\n");
    if (line) {
        serverLog(LL_WARNING, "Reading the manifest file, at line %d\n", linenum);
        serverLog(LL_WARNING, ">>> '%s'\n", line);
    }
    serverLog(LL_WARNING, "%s\n", err);
    exit(1);
}

/* 从 orig 深拷贝一个 aofManifest。
 *
 * 在 `backgroundRewriteDoneHandler` 和 `openNewIncrAofForAppend` 中，
 * 我们首先从 `server.aof_manifest` 深拷贝一个临时 AOF manifest 并尝试修改它。
 * 所有修改完成后，我们将原子地将 `server.aof_manifest` 指向这个临时 aof_manifest。
 */
aofManifest *aofManifestDup(aofManifest *orig) {
    serverAssert(orig != NULL);
    aofManifest *am = zcalloc(sizeof(aofManifest));

    am->curr_base_file_seq = orig->curr_base_file_seq;
    am->curr_incr_file_seq = orig->curr_incr_file_seq;
    am->dirty = orig->dirty;

    if (orig->base_aof_info) {
        am->base_aof_info = aofInfoDup(orig->base_aof_info);
    }

    am->incr_aof_list = listDup(orig->incr_aof_list);
    am->history_aof_list = listDup(orig->history_aof_list);
    serverAssert(am->incr_aof_list != NULL);
    serverAssert(am->history_aof_list != NULL);
    return am;
}

/* 将 `server.aof_manifest` 指针更改为 'am'，如果存在则释放之前的那个。 */
void aofManifestFreeAndUpdate(aofManifest *am) {
    serverAssert(am != NULL);
    if (server.aof_manifest) aofManifestFree(server.aof_manifest);
    server.aof_manifest = am;
}

/* 在 `backgroundRewriteDoneHandler` 中调用以获取新的 BASE 文件名，
 * 并将之前的（如果有）BASE 文件标记为 HISTORY 类型。
 *
 * BASE 文件命名规则：`server.aof_filename`.seq.base.format
 *
 * 例如：
 *  appendonly.aof.1.base.aof  （server.aof_use_rdb_preamble 为 no）
 *  appendonly.aof.1.base.rdb  （server.aof_use_rdb_preamble 为 yes）
 */
sds getNewBaseFileNameAndMarkPreAsHistory(aofManifest *am) {
    serverAssert(am != NULL);
    if (am->base_aof_info) {
        serverAssert(am->base_aof_info->file_type == AOF_FILE_TYPE_BASE);
        am->base_aof_info->file_type = AOF_FILE_TYPE_HIST;
        listAddNodeHead(am->history_aof_list, am->base_aof_info);
    }

    char *format_suffix = server.aof_use_rdb_preamble ?
        RDB_FORMAT_SUFFIX:AOF_FORMAT_SUFFIX;

    aofInfo *ai = aofInfoCreate();
    ai->file_name = sdscatprintf(sdsempty(), "%s.%lld%s%s", server.aof_filename,
                        ++am->curr_base_file_seq, BASE_FILE_SUFFIX, format_suffix);
    ai->file_seq = am->curr_base_file_seq;
    ai->file_type = AOF_FILE_TYPE_BASE;
    am->base_aof_info = ai;
    am->dirty = 1;
    return am->base_aof_info->file_name;
}

/* 获取新的 INCR 类型 AOF 名称。
 *
 * INCR AOF 命名规则：`server.aof_filename`.seq.incr.aof
 *
 * 例如：
 *  appendonly.aof.1.incr.aof
 */
sds getNewIncrAofName(aofManifest *am) {
    aofInfo *ai = aofInfoCreate();
    ai->file_type = AOF_FILE_TYPE_INCR;
    ai->file_name = sdscatprintf(sdsempty(), "%s.%lld%s%s", server.aof_filename,
                        ++am->curr_incr_file_seq, INCR_FILE_SUFFIX, AOF_FORMAT_SUFFIX);
    ai->file_seq = am->curr_incr_file_seq;
    listAddNodeTail(am->incr_aof_list, ai);
    am->dirty = 1;
    return ai->file_name;
}

/* 获取临时 INCR 类型 AOF 名称。 */
sds getTempIncrAofName(void) {
    return sdscatprintf(sdsempty(), "%s%s%s", TEMP_FILE_NAME_PREFIX, server.aof_filename,
        INCR_FILE_SUFFIX);
}

/* 获取最后一个 INCR AOF 名称或创建一个新的。 */
sds getLastIncrAofName(aofManifest *am) {
    serverAssert(am != NULL);

    /* 如果 'incr_aof_list' 为空，则创建一个新的。 */
    if (!listLength(am->incr_aof_list)) {
        return getNewIncrAofName(am);
    }

    /* 否则返回最后一个。 */
    listNode *lastnode = listIndex(am->incr_aof_list, -1);
    aofInfo *ai = listNodeValue(lastnode);
    return ai->file_name;
}

/* 在 `backgroundRewriteDoneHandler` 中调用。当 AOFRW 成功时，
 * 此函数会将 'incr_aof_list' 中的 AOF 文件类型从 AOF_FILE_TYPE_INCR
 * 更改为 AOF_FILE_TYPE_HIST，并将它们移至 'history_aof_list'。
 */
void markRewrittenIncrAofAsHistory(aofManifest *am) {
    serverAssert(am != NULL);
    if (!listLength(am->incr_aof_list)) {
        return;
    }

    listNode *ln;
    listIter li;

    listRewindTail(am->incr_aof_list, &li);

    /* "server.aof_fd != -1" 表示 AOF 已启用，那么我们必须跳过
     * 最后一个 AOF，因为该文件是我们当前正在写入的文件。 */
    if (server.aof_fd != -1) {
        ln = listNext(&li);
        serverAssert(ln != NULL);
    }

    /* 将 aofInfo 从 'incr_aof_list' 移动到 'history_aof_list'。 */
    while ((ln = listNext(&li)) != NULL) {
        aofInfo *ai = (aofInfo*)ln->value;
        serverAssert(ai->file_type == AOF_FILE_TYPE_INCR);

        aofInfo *hai = aofInfoDup(ai);
        hai->file_type = AOF_FILE_TYPE_HIST;
        listAddNodeHead(am->history_aof_list, hai);
        listDelNode(am->incr_aof_list, ln);
    }

    am->dirty = 1;
}

/* 将格式化后的 manifest 字符串写入磁盘。 */
int writeAofManifestFile(sds buf) {
    int ret = C_OK;
    ssize_t nwritten;
    int len;

    sds am_name = getAofManifestFileName();
    sds am_filepath = makePath(server.aof_dirname, am_name);
    sds tmp_am_name = getTempAofManifestFileName();
    sds tmp_am_filepath = makePath(server.aof_dirname, tmp_am_name);

    int fd = open(tmp_am_filepath, O_WRONLY|O_TRUNC|O_CREAT, 0644);
    if (fd == -1) {
        serverLog(LL_WARNING, "Can't open the AOF manifest file %s: %s",
            tmp_am_name, strerror(errno));

        ret = C_ERR;
        goto cleanup;
    }

    len = sdslen(buf);
    while(len) {
        nwritten = write(fd, buf, len);

        if (nwritten < 0) {
            if (errno == EINTR) continue;

            serverLog(LL_WARNING, "Error trying to write the temporary AOF manifest file %s: %s",
                tmp_am_name, strerror(errno));

            ret = C_ERR;
            goto cleanup;
        }

        len -= nwritten;
        buf += nwritten;
    }

    if (redis_fsync(fd) == -1) {
        serverLog(LL_WARNING, "Fail to fsync the temp AOF file %s: %s.",
            tmp_am_name, strerror(errno));

        ret = C_ERR;
        goto cleanup;
    }

    if (rename(tmp_am_filepath, am_filepath) != 0) {
        serverLog(LL_WARNING,
            "Error trying to rename the temporary AOF manifest file %s into %s: %s",
            tmp_am_name, am_name, strerror(errno));

        ret = C_ERR;
        goto cleanup;
    }

    /* 同时同步 AOF 目录，因为 AOF 目录中可能新增了 AOF 文件 */
    if (fsyncFileDir(am_filepath) == -1) {
        serverLog(LL_WARNING, "Fail to fsync AOF directory %s: %s.",
            am_filepath, strerror(errno));

        ret = C_ERR;
        goto cleanup;
    }

cleanup:
    if (fd != -1) close(fd);
    sdsfree(am_name);
    sdsfree(am_filepath);
    sdsfree(tmp_am_name);
    sdsfree(tmp_am_filepath);
    return ret;
}

/* 将由 am 指向的 aofManifest 信息持久化到磁盘。 */
int persistAofManifest(aofManifest *am) {
    if (am->dirty == 0) {
        return C_OK;
    }

    sds amstr = getAofManifestAsString(am);
    int ret = writeAofManifestFile(amstr);
    sdsfree(amstr);
    if (ret == C_OK) am->dirty = 0;
    return ret;
}

/* 在我们从旧版本 redis 升级时在 `loadAppendOnlyFiles` 中调用。
 *
 * 1) 使用 'server.aof_dirname' 作为名称创建 AOF 目录。
 * 2) 使用 'server.aof_filename' 构造一个 BASE 类型的 aofInfo 并将其添加到
 *    aofManifest，然后将 manifest 文件持久化到 AOF 目录。
 * 3) 将旧 AOF 文件（server.aof_filename）移动到 AOF 目录。
 *
 * 如果上述步骤失败或发生崩溃，这不会导致任何问题，redis 将在重启时重试升级过程。
 */
void aofUpgradePrepare(aofManifest *am) {
    serverAssert(!aofFileExist(server.aof_filename));

    /* 使用 'server.aof_dirname' 作为名称创建 AOF 目录。 */
    if (dirCreateIfMissing(server.aof_dirname) == -1) {
        serverLog(LL_WARNING, "Can't open or create append-only dir %s: %s",
            server.aof_dirname, strerror(errno));
        exit(1);
    }

    /* 手动构造一个 BASE 类型的 aofInfo 并将其添加到 aofManifest。 */
    if (am->base_aof_info) aofInfoFree(am->base_aof_info);
    aofInfo *ai = aofInfoCreate();
    ai->file_name = sdsnew(server.aof_filename);
    ai->file_seq = 1;
    ai->file_type = AOF_FILE_TYPE_BASE;
    am->base_aof_info = ai;
    am->curr_base_file_seq = 1;
    am->dirty = 1;

    /* 将 manifest 文件持久化到 AOF 目录。 */
    if (persistAofManifest(am) != C_OK) {
        exit(1);
    }

    /* 将旧 AOF 文件移动到 AOF 目录。 */
    sds aof_filepath = makePath(server.aof_dirname, server.aof_filename);
    if (rename(server.aof_filename, aof_filepath) == -1) {
        serverLog(LL_WARNING,
            "Error trying to move the old AOF file %s into dir %s: %s",
            server.aof_filename,
            server.aof_dirname,
            strerror(errno));
        sdsfree(aof_filepath);
        exit(1);
    }
    sdsfree(aof_filepath);

    serverLog(LL_NOTICE, "Successfully migrated an old-style AOF file (%s) into the AOF directory (%s).",
        server.aof_filename, server.aof_dirname);
}

/* 当 AOFRW 成功时，之前的 BASE 和 INCR AOF 将变为 HISTORY 类型并被移动到 'history_aof_list'。
 *
 * 该函数将遍历 'history_aof_list' 并将删除任务提交给 bio 线程。
 */
int aofDelHistoryFiles(void) {
    if (server.aof_manifest == NULL ||
        server.aof_disable_auto_gc == 1 ||
        !listLength(server.aof_manifest->history_aof_list))
    {
        return C_OK;
    }

    listNode *ln;
    listIter li;

    listRewind(server.aof_manifest->history_aof_list, &li);
    while ((ln = listNext(&li)) != NULL) {
        aofInfo *ai = (aofInfo*)ln->value;
        serverAssert(ai->file_type == AOF_FILE_TYPE_HIST);
        serverLog(LL_NOTICE, "Removing the history file %s in the background", ai->file_name);
        sds aof_filepath = makePath(server.aof_dirname, ai->file_name);
        bg_unlink(aof_filepath);
        sdsfree(aof_filepath);
        listDelNode(server.aof_manifest->history_aof_list, ln);
    }

    server.aof_manifest->dirty = 1;
    return persistAofManifest(server.aof_manifest);
}

/* 用于在 AOFRW 失败时清理临时 INCR AOF。 */
void aofDelTempIncrAofFile(void) {
    sds aof_filename = getTempIncrAofName();
    sds aof_filepath = makePath(server.aof_dirname, aof_filename);
    serverLog(LL_NOTICE, "Removing the temp incr aof file %s in the background", aof_filename);
    bg_unlink(aof_filepath);
    sdsfree(aof_filepath);
    sdsfree(aof_filename);
    return;
}

/* 在 redis 启动时于 `loadDataFromDisk` 之后调用。如果 `server.aof_state` 是
 * 'AOF_ON'，它将执行三件事：
 * 1. 当 redis 以空数据集启动时强制创建 BASE 文件
 * 2. 打开最近打开的 INCR 类型 AOF 用于写入，如果没有则创建一个新的
 * 3. 将 manifest 文件同步更新到磁盘
 *
 * 如果上述步骤失败，redis 进程将退出。
 */
void aofOpenIfNeededOnServerStart(void) {
    if (server.aof_state != AOF_ON) {
        return;
    }

    serverAssert(server.aof_manifest != NULL);
    serverAssert(server.aof_fd == -1);

    if (dirCreateIfMissing(server.aof_dirname) == -1) {
        serverLog(LL_WARNING, "Can't open or create append-only dir %s: %s",
            server.aof_dirname, strerror(errno));
        exit(1);
    }

    /* 如果我们以空数据集启动，将强制创建一个 BASE 文件。 */
    size_t incr_aof_len = listLength(server.aof_manifest->incr_aof_list);
    if (!server.aof_manifest->base_aof_info && !incr_aof_len) {
        sds base_name = getNewBaseFileNameAndMarkPreAsHistory(server.aof_manifest);
        sds base_filepath = makePath(server.aof_dirname, base_name);
        if (rewriteAppendOnlyFile(base_filepath) != C_OK) {
            exit(1);
        }
        sdsfree(base_filepath);
        serverLog(LL_NOTICE, "Creating AOF base file %s on server start",
            base_name);
    }

    /* 因为如果打开 AOF 或持久化 manifest 失败我们将 'exit(1)'，
     * 所以此处不需要原子修改。 */
    sds aof_name = getLastIncrAofName(server.aof_manifest);

    /* 这里我们应该使用 'O_APPEND' 标志。 */
    sds aof_filepath = makePath(server.aof_dirname, aof_name);
    server.aof_fd = open(aof_filepath, O_WRONLY|O_APPEND|O_CREAT, 0644);
    sdsfree(aof_filepath);
    if (server.aof_fd == -1) {
        serverLog(LL_WARNING, "Can't open the append-only file %s: %s",
            aof_name, strerror(errno));
        exit(1);
    }

    /* 持久化我们的更改。 */
    int ret = persistAofManifest(server.aof_manifest);
    if (ret != C_OK) {
        exit(1);
    }

    server.aof_last_incr_size = getAppendOnlyFileSize(aof_name, NULL);
    server.aof_last_incr_fsync_offset = server.aof_last_incr_size;

    if (incr_aof_len) {
        serverLog(LL_NOTICE, "Opening AOF incr file %s on server start", aof_name);
    } else {
        serverLog(LL_NOTICE, "Creating AOF incr file %s on server start", aof_name);
    }
}

/* 检查指定的 AOF 文件是否存在。
 * 返回 1 表示存在，0 表示不存在。 */
int aofFileExist(char *filename) {
    sds file_path = makePath(server.aof_dirname, filename);
    int ret = fileExist(file_path);
    sdsfree(file_path);
    return ret;
}

/* 在 `rewriteAppendOnlyFileBackground` 中调用。如果 `server.aof_state` 是 'AOF_ON'，
 * 它将执行两件事：
 * 1. 打开一个新的 INCR 类型 AOF 用于写入
 * 2. 将 manifest 文件同步更新到磁盘
 *
 * 上述两步修改是原子的，也就是说，如果任何步骤失败，整个操作将回滚并返回 C_ERR，
 * 如果全部成功则返回 C_OK。
 *
 * 如果 `server.aof_state` 是 'AOF_WAIT_REWRITE'，它将打开一个临时 INCR AOF 文件
 * 来在 AOF_WAIT_REWRITE 期间累积数据，并最终在 `backgroundRewriteDoneHandler` 中
 * 被重命名并写入 manifest 文件。
 * */
int openNewIncrAofForAppend(void) {
    serverAssert(server.aof_manifest != NULL);
    int newfd = -1;
    aofManifest *temp_am = NULL;
    sds new_aof_name = NULL;

    /* 仅当 AOF 启用时打开新的 INCR AOF。 */
    if (server.aof_state == AOF_OFF) return C_OK;

    /* 打开新的 AOF。 */
    if (server.aof_state == AOF_WAIT_REWRITE) {
        /* 使用临时 INCR AOF 文件来在 AOF_WAIT_REWRITE 期间累积数据。 */
        new_aof_name = getTempIncrAofName();
    } else {
        /* 复制一个临时的 aof_manifest 以进行修改。 */
        temp_am = aofManifestDup(server.aof_manifest);
        new_aof_name = sdsdup(getNewIncrAofName(temp_am));
    }
    sds new_aof_filepath = makePath(server.aof_dirname, new_aof_name);
    newfd = open(new_aof_filepath, O_WRONLY|O_TRUNC|O_CREAT, 0644);
    sdsfree(new_aof_filepath);
    if (newfd == -1) {
        serverLog(LL_WARNING, "Can't open the append-only file %s: %s",
            new_aof_name, strerror(errno));
        goto cleanup;
    }

    if (temp_am) {
        /* 持久化 AOF Manifest。 */
        if (persistAofManifest(temp_am) == C_ERR) {
            goto cleanup;
        }
    }

    serverLog(LL_NOTICE, "Creating AOF incr file %s on background rewrite",
            new_aof_name);
    sdsfree(new_aof_name);

    /* 如果到达此处，我们可以安全地修改 `server.aof_manifest` 和 `server.aof_fd`。 */

    /* 如果需要，fsync 并关闭旧的 aof_fd。在 everysec 的 fsync 策略下，只要我们保证
     * fsync 最终会发生，推迟 fsync 是可以的；而在 always 策略下，此时文件已经同步，
     * 所以 fsync 没有影响。 */
    if (server.aof_fd != -1) {
        aof_background_fsync_and_close(server.aof_fd);
        server.aof_last_fsync = server.mstime;
    }
    server.aof_fd = newfd;

    /* 重置 aof_last_incr_size。 */
    server.aof_last_incr_size = 0;
    /* 重置 aof_last_incr_fsync_offset。 */
    server.aof_last_incr_fsync_offset = 0;
    /* 更新 `server.aof_manifest`。 */
    if (temp_am) aofManifestFreeAndUpdate(temp_am);
    return C_OK;

cleanup:
    if (new_aof_name) sdsfree(new_aof_name);
    if (newfd != -1) close(newfd);
    if (temp_am) aofManifestFree(temp_am);
    return C_ERR;
}

/* 是否限制后台 AOF 重写的执行。
 *
 * 目前，如果 AOFRW 失败，redis 将自动重试。如果持续失败，
 * 我们可能会得到大量非常小的 INCR 文件。因此我们需要一个 AOFRW 限制措施。
 *
 * 我们不能直接使用 `server.aof_current_size` 和 `server.aof_last_incr_size`，
 * 因为在 AOFRW 失败后可能没有新的写入。
 *
 * 因此，我们使用时间延迟来实现目标。当 AOFRW 失败时，我们将下一次 AOFRW 的执行
 * 延迟 1 分钟。如果下一次 AOFRW 也失败，将延迟 2 分钟。接下来是 4、8、16，
 * 最大延迟为 60 分钟（1 小时）。
 *
 * 在限制期内，我们仍然可以使用 'bgrewriteaof' 命令立即执行 AOFRW。
 *
 * 返回 1 表示 AOFRW 被限制无法执行。返回 0 表示我们可以执行 AOFRW，
 * 这可能是因为我们已达到 'next_rewrite_time'，或者 INCR AOF 的数量
 * 还未达到限制阈值。
 * */
#define AOF_REWRITE_LIMITE_THRESHOLD    3
#define AOF_REWRITE_LIMITE_MAX_MINUTES  60 /* 1 hour */
int aofRewriteLimited(void) {
    static int next_delay_minutes = 0;
    static time_t next_rewrite_time = 0;

    if (server.stat_aofrw_consecutive_failures < AOF_REWRITE_LIMITE_THRESHOLD) {
        /* 我们可能正在从限制状态恢复，因此重置所有状态。 */
        next_delay_minutes = 0;
        next_rewrite_time = 0;
        return 0;
    }

    /* 如果处于限制状态，则检查是否已达到 next_rewrite_time */
    if (next_rewrite_time != 0) {
        if (server.unixtime < next_rewrite_time) {
            return 1;
        } else {
            next_rewrite_time = 0;
            return 0;
        }
    }

    next_delay_minutes = (next_delay_minutes == 0) ? 1 : (next_delay_minutes * 2);
    if (next_delay_minutes > AOF_REWRITE_LIMITE_MAX_MINUTES) {
        next_delay_minutes = AOF_REWRITE_LIMITE_MAX_MINUTES;
    }

    next_rewrite_time = server.unixtime + next_delay_minutes * 60;
    serverLog(LL_WARNING,
        "Background AOF rewrite has repeatedly failed and triggered the limit, will retry in %d minutes", next_delay_minutes);
    return 1;
}

/* ----------------------------------------------------------------------------
 * AOF 文件实现
 * ------------------------------------------------------------------------- */

/* 如果 AOF fsync 当前已在 BIO 线程中进行，则返回 true。 */
int aofFsyncInProgress(void) {
    /* 注意我们不关心 aof_background_fsync_and_close，
     * 因为 server.aof_fd 已被新的 INCR AOF 文件 fd 替换，
     * 请参阅 openNewIncrAofForAppend。 */
    return bioPendingJobsOfType(BIO_AOF_FSYNC) != 0;
}

/* 启动一个后台任务，在另一个线程中针对指定文件描述符（AOF 文件的描述符）执行 fsync()。 */
void aof_background_fsync(int fd) {
    bioCreateFsyncJob(fd, server.master_repl_offset, 1);
}

/* 在 aof_background_fsync 的基础上关闭 fd。 */
void aof_background_fsync_and_close(int fd) {
    bioCreateCloseAofJob(fd, server.master_repl_offset, 1);
}

/* 如果存在 AOFRW 子进程，则将其终止 */
void killAppendOnlyChild(void) {
    int statloc;
    /* 没有 AOFRW 子进程？返回。 */
    if (server.child_type != CHILD_TYPE_AOF) return;
    /* 终止 AOFRW 子进程，等待子进程退出。 */
    serverLog(LL_NOTICE,"Killing running AOF rewrite child: %ld",
        (long) server.child_pid);
    if (kill(server.child_pid,SIGUSR1) != -1) {
        while(waitpid(-1, &statloc, 0) != server.child_pid);
    }
    aofRemoveTempFile(server.child_pid);
    resetChildState();
    server.aof_rewrite_time_start = -1;
}

/* 当用户在运行时通过 CONFIG 命令从 "appendonly yes" 切换到 "appendonly no" 时调用。 */
void stopAppendOnly(void) {
    serverAssert(server.aof_state != AOF_OFF);
    flushAppendOnlyFile(1);
    if (redis_fsync(server.aof_fd) == -1) {
        serverLog(LL_WARNING,"Fail to fsync the AOF file: %s",strerror(errno));
    } else {
        server.aof_last_fsync = server.mstime;
    }
    close(server.aof_fd);

    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = AOF_OFF;
    server.aof_rewrite_scheduled = 0;
    server.aof_last_incr_size = 0;
    server.aof_last_incr_fsync_offset = 0;
    server.fsynced_reploff = -1;
    atomicSet(server.fsynced_reploff_pending, 0);
    killAppendOnlyChild();
    sdsfree(server.aof_buf);
    server.aof_buf = sdsempty();
}

/* 当用户在运行时通过 CONFIG 命令从 "appendonly no" 切换到 "appendonly yes" 时调用。 */
int startAppendOnly(void) {
    serverAssert(server.aof_state == AOF_OFF);

    server.aof_state = AOF_WAIT_REWRITE;
    if (hasActiveChildProcess() && server.child_type != CHILD_TYPE_AOF) {
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_NOTICE,"AOF was enabled but there is already another background operation. An AOF background was scheduled to start when possible.");
    } else if (server.in_exec){
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_NOTICE,"AOF was enabled during a transaction. An AOF background was scheduled to start when possible.");
    } else {
        /* 如果存在待处理的 AOF 重写，我们需要将其关闭并启动一个新的：
         * 旧的重写不能被重用，因为它没有累积 AOF 缓冲区。 */
        if (server.child_type == CHILD_TYPE_AOF) {
            serverLog(LL_NOTICE,"AOF was enabled but there is already an AOF rewriting in background. Stopping background AOF and starting a rewrite now.");
            killAppendOnlyChild();
        }

        if (rewriteAppendOnlyFileBackground() == C_ERR) {
            server.aof_state = AOF_OFF;
            serverLog(LL_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
            return C_ERR;
        }
    }
    server.aof_last_fsync = server.mstime;
    /* 如果 bio 任务中 AOF fsync 出错，我们仅忽略它并记录事件。 */
    int aof_bio_fsync_status;
    atomicGet(server.aof_bio_fsync_status, aof_bio_fsync_status);
    if (aof_bio_fsync_status == C_ERR) {
        serverLog(LL_WARNING,
            "AOF reopen, just ignore the AOF fsync error in bio job");
        atomicSet(server.aof_bio_fsync_status,C_OK);
    }

    /* 如果 AOF 处于错误状态，我们仅忽略它并记录事件。 */
    if (server.aof_last_write_status == C_ERR) {
        serverLog(LL_WARNING,"AOF reopen, just ignore the last error.");
        server.aof_last_write_status = C_OK;
    }
    return C_OK;
}

/* 这是对 write 系统调用的包装器，用于在短写入或系统调用被中断时重试。
 * 对于我们在写入块设备时对短写入进行重试可能看起来奇怪：通常如果第一次调用是短写入，
 * 则表明是空间耗尽的情况，所以下一次很可能也会失败。然而在现代系统中这似乎不再成立，
 * 总的来说，重试写入看起来更具弹性。如果存在实际的错误条件，我们将在下一次尝试中获取它。 */
ssize_t aofWrite(int fd, const char *buf, size_t len) {
    ssize_t nwritten = 0, totwritten = 0;

    while(len) {
        nwritten = write(fd, buf, len);

        if (nwritten < 0) {
            if (errno == EINTR) continue;
            return totwritten ? totwritten : -1;
        }

        len -= nwritten;
        buf += nwritten;
        totwritten += nwritten;
    }

    return totwritten;
}

/* 将 AOF 缓冲区内容写入磁盘。
 *
 * 由于我们需要在回复客户端之前写入 AOF，而客户端 socket 只能在进入事件循环
 * 时获得写入，因此我们将所有 AOF 写入累积在内存缓冲区中，并在重新进入事件循环之前
 * 使用此函数将其写入磁盘。
 *
 * 关于 'force' 参数：
 *
 * 当 fsync 策略设置为 'everysec' 时，如果后台线程中仍有 fsync() 在进行，
 * 我们可能会延迟刷新，例如在 Linux 上，write(2) 无论如何都会被后台 fsync 阻塞。
 * 发生这种情况时，我们会记住有一些 AOF 缓冲区需要尽快刷新，并将在 serverCron()
 * 函数中尝试执行此操作。
 *
 * 但是，如果 force 设置为 1，我们将无视后台 fsync 立即写入。 */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* 错误日志记录之间的秒数。 */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;

    if (sdslen(server.aof_buf) == 0) {
        if (server.aof_last_incr_fsync_offset == server.aof_last_incr_size) {
            /* 所有数据已经 fsync：以防万一更新 fsynced_reploff_pending。
             * 这对于避免 WAITAOF 挂起是必要的，以防模块使用 RM_Call 并带上 NO_AOF 标志，
             * 在这种情况下 master_repl_offset 将增加，但 fsynced_reploff_pending 不会更新
             * （因为从 AOF 角度来看，没有理由调用 fsync），然后 WAITAOF 可能会等待更高的偏移
             * （该偏移包含仅传播到副本而非 AOF 的数据）。 */
            if (!aofFsyncInProgress())
                atomicSet(server.fsynced_reploff_pending, server.master_repl_offset);
        } else {
            /* 检查即使 aof 缓冲区为空是否也需要进行 fsync，
             * 因为之前在 AOF_FSYNC_EVERYSEC 模式下，fsync 仅在 aof 缓冲区非空时
             * 被调用，所以如果用户在一秒内的 fsync 调用之前停止写命令，
             * 页缓存中的数据无法及时刷新。 */
            if (server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.mstime - server.aof_last_fsync >= 1000 &&
                !(sync_in_progress = aofFsyncInProgress()))
                goto try_fsync;

            /* 检查即使 aof 缓冲区为空是否也需要进行 fsync，原因在前一个
             * AOF_FSYNC_EVERYSEC 块中已经说明，此处也检查 AOF_FSYNC_ALWAYS
             * 以处理 aof_fsync 从 everysec 更改为 always 的情况。 */
            if (server.aof_fsync == AOF_FSYNC_ALWAYS)
                goto try_fsync;
        }
        return;
    }

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        sync_in_progress = aofFsyncInProgress();

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        /* 使用此追加 fsync 策略时，我们执行后台 fsync。
         * 如果 fsync 仍在进行，我们可以尝试将写入延迟几秒钟。 */
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                /* 之前没有推迟写入，记录我们正在推迟刷新并返回。 */
                server.aof_flush_postponed_start = server.mstime;
                return;
            } else if (server.mstime - server.aof_flush_postponed_start < 2000) {
                /* 我们已经在等待 fsync 完成，但少于两秒仍然可以，再次推迟。 */
                return;
            }
            /* 否则继续往下走，执行写入，因为我们不能等待超过两秒。 */
            server.aof_delayed_fsync++;
            serverLog(LL_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    /* 我们希望执行单次写入。这至少在我们写入的是真实物理文件系统时应该是原子的。
     * 虽然这可以保护我们免受服务器被 kill 的影响，但对于服务器因电源问题而停止等情况，
     * 我认为我们也无能为力。 */

    if (server.aof_flush_sleep && sdslen(server.aof_buf)) {
        usleep(server.aof_flush_sleep);
    }

    latencyStartMonitor(latency);
    nwritten = aofWrite(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
    latencyEndMonitor(latency);
    /* 我们希望捕获延迟写入的不同事件：
     * 延迟发生时是否有待处理的 fsync，或是否有正在活动的保存子进程，
     * 以及上述两个条件都不存在的情况。
     * 我们还使用一个额外的事件名称来保存所有样本，这对绘图/监视很有用。 */
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync",latency);
    } else if (hasActiveChildProcess()) {
        latencyAddSampleIfNeeded("aof-write-active-child",latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone",latency);
    }
    latencyAddSampleIfNeeded("aof-write",latency);

    /* 我们已执行写入，因此将推迟刷新哨兵重置为零。 */
    server.aof_flush_postponed_start = 0;

    if (nwritten != (ssize_t)sdslen(server.aof_buf)) {
        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* 将日志记录速率限制为每 AOF_WRITE_LOG_ERROR_RATE 秒 1 行。 */
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* 记录 AOF 写入错误并记录错误代码。 */
        if (nwritten == -1) {
            if (can_log) {
                serverLog(LL_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
            }
            server.aof_last_write_errno = errno;
        } else {
            if (can_log) {
                serverLog(LL_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            if (ftruncate(server.aof_fd, server.aof_last_incr_size) == -1) {
                if (can_log) {
                    serverLog(LL_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* 如果 ftruncate() 成功，我们可以将 nwritten 设置为 -1，
                 * 因为 AOF 中不再有部分数据。 */
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* 处理 AOF 写入错误。 */
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* 当 fsync 策略为 ALWAYS 时我们无法恢复，因为对客户端的回复已经在输出缓冲区中
             * （包括写入和读取），而对数据库的更改无法回滚。由于我们与用户有约定，
             * 即已确认或观察到的写入都已同步到磁盘，我们必须退出。 */
            serverLog(LL_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* 从失败的写入中恢复，将数据留在缓冲区中。然而设置一个错误，
             * 以在错误条件未清除之前停止接受写入。 */
            server.aof_last_write_status = C_ERR;

            /* 如果发生了部分写入且无法使用 ftruncate(2) 撤消，则修剪 sds 缓冲区。 */
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                server.aof_last_incr_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            return; /* 我们将在下一次调用时重试... */
        }
    } else {
        /* 写入(2)成功。如果 AOF 处于错误状态，恢复 OK 状态并记录事件。 */
        if (server.aof_last_write_status == C_ERR) {
            serverLog(LL_NOTICE,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = C_OK;
        }
    }
    server.aof_current_size += nwritten;
    server.aof_last_incr_size += nwritten;

    /* 当 AOF 缓冲区足够小时重复使用。最大值来自 4k 的 arena 减去一些开销
     * （但除此之外是任意的）。 */
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

try_fsync:
    /* 如果 no-appendfsync-on-rewrite 设置为 yes 且后台有子进程在进行 I/O，则不执行 fsync。 */
    if (server.aof_no_fsync_on_rewrite && hasActiveChildProcess())
        return;

    /* 如果需要，执行 fsync。 */
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* redis_fsync 在 Linux 上定义为 fdatasync()，以避免刷新元数据。 */
        latencyStartMonitor(latency);
        /* 让我们尝试将此数据放到磁盘上。为了在 AOF fsync 策略为 'always' 时保证数据安全，
         * 如果 fsync AOF 失败我们应该退出（参见上面写错误后 exit(1) 旁边的注释）。 */
        if (redis_fsync(server.aof_fd) == -1) {
            serverLog(LL_WARNING,"Can't persist AOF for fsync error when the "
              "AOF fsync policy is 'always': %s. Exiting...", strerror(errno));
            exit(1);
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always",latency);
        server.aof_last_incr_fsync_offset = server.aof_last_incr_size;
        server.aof_last_fsync = server.mstime;
        atomicSet(server.fsynced_reploff_pending, server.master_repl_offset);
    } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC &&
               server.mstime - server.aof_last_fsync >= 1000) {
        if (!sync_in_progress) {
            aof_background_fsync(server.aof_fd);
            server.aof_last_incr_fsync_offset = server.aof_last_incr_size;
        }
        server.aof_last_fsync = server.mstime;
    }
}

/* 将给定的命令参数数组序列化为 RESP 格式并拼接到 dst。
 * argc 是参数个数，argv 是参数对象数组。 */
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        decrRefCount(o);
    }
    return dst;
}

/* 如果当前 AOF 中记录的时间戳与服务器 unix 时间不相等，则为 AOF 生成一段时间戳注释。
 * 如果我们将 'force' 参数指定为 1，我们将无需检查即生成一个时间戳注释，
 * 目前它在 AOF 重写子进程中很有用，该子进程总是在重写 AOF 开始时需要记录一个时间戳。
 *
 * 时间戳注释格式为 "#TS:${timestamp}\r\n"。"TS" 是 timestamp 的缩写，
 * 这种方法可以节省 AOF 中的额外字节。 */
sds genAofTimestampAnnotationIfNeeded(int force) {
    sds ts = NULL;

    if (force || server.aof_cur_timestamp < server.unixtime) {
        server.aof_cur_timestamp = force ? time(NULL) : server.unixtime;
        ts = sdscatfmt(sdsempty(), "#TS:%I\r\n", server.aof_cur_timestamp);
        serverAssert(sdslen(ts) <= AOF_ANNOTATION_LINE_MAX_LEN);
    }
    return ts;
}

/* 将给定的命令写入 aof 文件。
 * dictid - 命令应应用的字典 ID，
 *          用于决定是否应将 `select` 命令也写入 aof。值为 -1 表示
 *          在任何情况下都避免写入 `select` 命令。
 * argv   - 要写入 aof 的命令。
 * argc   - argv 中的值数量
 */
void feedAppendOnlyFile(int dictid, robj **argv, int argc) {
    sds buf = sdsempty();

    serverAssert(dictid == -1 || (dictid >= 0 && dictid < server.dbnum));

    /* 如果需要则写入时间戳 */
    if (server.aof_timestamp_enabled) {
        sds ts = genAofTimestampAnnotationIfNeeded(0);
        if (ts != NULL) {
            buf = sdscatsds(buf, ts);
            sdsfree(ts);
        }
    }

    /* 此命令的目标 DB 与我们追加的上一条命令不同。需要发出 SELECT 命令。 */
    if (dictid != -1 && dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    /* 所有命令在 AOF 中应该以与复制相同的方式进行传播。
     * 无需进行 AOF 特定的转换。 */
    buf = catAppendOnlyGenericCommand(buf,argc,argv);

    /* 追加到 AOF 缓冲区。这将在重新进入事件循环之前刷新到磁盘，
     * 因此在客户端收到关于所执行操作的肯定回复之前完成。 */
    if (server.aof_state == AOF_ON ||
        (server.aof_state == AOF_WAIT_REWRITE && server.child_type == CHILD_TYPE_AOF))
    {
        server.aof_buf = sdscatlen(server.aof_buf, buf, sdslen(buf));
    }

    sdsfree(buf);
}

/* ----------------------------------------------------------------------------
 * AOF 加载
 * ------------------------------------------------------------------------- */

/* 在 Redis 中，命令总是在客户端的上下文中执行，因此为了加载 AOF 文件，
 * 我们需要创建一个伪客户端。 */
struct client *createAOFClient(void) {
    struct client *c = createClient(NULL);

    c->id = CLIENT_ID_AOF; /* 这样模块就可以识别它是 AOF 客户端。 */

    /*
     * AOF 客户端绝不应该被阻塞（与主复制连接不同）。
     * 这是因为阻塞 AOF 客户端可能导致死锁（因为可能没有人会解除它的阻塞）。
     * 此外，如果 AOF 客户端仅由于后台处理而被阻塞，
     * 有可能违反命令执行顺序。
     */
    c->flags = CLIENT_DENY_BLOCKING;

    /* 我们将伪客户端设置为等待同步的从服务器，
     * 以便 Redis 不会尝试向此客户端发送回复。 */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    return c;
}

/* 重放 AOF 文件。成功时返回 AOF_OK 或 AOF_TRUNCATED，否则返回以下之一：
 * AOF_OPEN_ERR：无法打开 AOF 文件。
 * AOF_NOT_EXIST：AOF 文件不存在。
 * AOF_EMPTY：AOF 文件为空（无内容可加载）。
 * AOF_FAILED：加载 AOF 文件失败。 */
int loadSingleAppendOnlyFile(char *filename) {
    struct client *fakeClient;
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* 已加载的最新格式良好的命令的偏移量。 */
    off_t valid_before_multi = 0; /* 已加载 MULTI 命令之前的偏移量。 */
    off_t last_progress_report_size = 0;
    int ret = AOF_OK;

    sds aof_filepath = makePath(server.aof_dirname, filename);
    FILE *fp = fopen(aof_filepath, "r");
    if (fp == NULL) {
        int en = errno;
        if (redis_stat(aof_filepath, &sb) == 0 || errno != ENOENT) {
            serverLog(LL_WARNING,"Fatal error: can't open the append log file %s for reading: %s", filename, strerror(en));
            sdsfree(aof_filepath);
            return AOF_OPEN_ERR;
        } else {
            serverLog(LL_WARNING,"The append log file %s doesn't exist: %s", filename, strerror(errno));
            sdsfree(aof_filepath);
            return AOF_NOT_EXIST;
        }
    }

    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        fclose(fp);
        sdsfree(aof_filepath);
        return AOF_EMPTY;
    }

    /* 临时禁用 AOF，以防止 EXEC 将 MULTI 馈送到我们即将读取的同一文件中。 */
    server.aof_state = AOF_OFF;

    client *old_cur_client = server.current_client;
    client *old_exec_client = server.executing_client;
    fakeClient = createAOFClient();
    server.current_client = server.executing_client = fakeClient;

    /* 检查 AOF 文件是否为 RDB 格式（它可能是 RDB 编码的 BASE AOF
     * 或旧式 RDB-preamble AOF）。在这种情况下，我们需要加载 RDB 文件，
     * 如果是旧式 RDB-preamble AOF，则稍后继续加载 AOF 尾部。 */
    char sig[5]; /* "REDIS" */
    if (fread(sig,1,5,fp) != 5 || memcmp(sig,"REDIS",5) != 0) {
        /* 不是 RDB 格式，回到 0 偏移位置。 */
        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
    } else {
        /* RDB 格式。交给 RDB 加载函数处理。 */
        rio rdb;
        int old_style = !strcmp(filename, server.aof_filename);
        if (old_style)
            serverLog(LL_NOTICE, "Reading RDB preamble from AOF file...");
        else
            serverLog(LL_NOTICE, "Reading RDB base file on AOF loading...");

        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
        rioInitWithFile(&rdb,fp);
        if (rdbLoadRio(&rdb,RDBFLAGS_AOF_PREAMBLE,NULL) != C_OK) {
            if (old_style)
                serverLog(LL_WARNING, "Error reading the RDB preamble of the AOF file %s, AOF loading aborted", filename);
            else
                serverLog(LL_WARNING, "Error reading the RDB base file %s, AOF loading aborted", filename);

            ret = AOF_FAILED;
            goto cleanup;
        } else {
            loadingAbsProgress(ftello(fp));
            last_progress_report_size = ftello(fp);
            if (old_style) serverLog(LL_NOTICE, "Reading the remaining AOF tail...");
        }
    }

    /* 逐个命令读取实际的 AOF 文件，格式为 REPL。 */
    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[AOF_ANNOTATION_LINE_MAX_LEN];
        sds argsds;
        struct redisCommand *cmd;

        /* 定期为客户端提供服务 */
        if (!(loops++ % 1024)) {
            off_t progress_delta = ftello(fp) - last_progress_report_size;
            loadingIncrProgress(progress_delta);
            last_progress_report_size += progress_delta;
            processEventsWhileBlocked();
            processModuleLoadingProgressEvent(1);
        }
        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp)) {
                break;
            } else {
                goto readerr;
            }
        }
        if (buf[0] == '#') continue; /* 跳过注释 */
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;
        if ((size_t)argc > SIZE_MAX / sizeof(robj*)) goto fmterr;

        /* 将 AOF 中的下一条命令加载为伪客户端的 argv。 */
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        fakeClient->argv_len = argc;

        for (j = 0; j < argc; j++) {
            /* 解析参数长度。 */
            char *readres = fgets(buf,sizeof(buf),fp);
            if (readres == NULL || buf[0] != '$') {
                fakeClient->argc = j; /* 释放 j-1 之前的元素。 */
                freeClientArgv(fakeClient);
                if (readres == NULL)
                    goto readerr;
                else
                    goto fmterr;
            }
            len = strtol(buf+1,NULL,10);

            /* 将其读取到字符串对象中。 */
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* 释放 j-1 之前的元素。 */
                freeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(OBJ_STRING,argsds);

            /* 丢弃 CRLF。 */
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* 释放 j 之前的元素。 */
                freeClientArgv(fakeClient);
                goto readerr;
            }
        }

        /* 命令查找 */
        cmd = lookupCommand(argv,argc);
        if (!cmd) {
            serverLog(LL_WARNING,
                "Unknown command '%s' reading the append only file %s",
                (char*)argv[0]->ptr, filename);
            freeClientArgv(fakeClient);
            ret = AOF_FAILED;
            goto cleanup;
        }

        if (cmd->proc == multiCommand) valid_before_multi = valid_up_to;

        /* 在伪客户端的上下文中运行命令 */
        fakeClient->cmd = fakeClient->lastcmd = cmd;
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand)
        {
            /* 注意：我们不必尝试调用 evalGetCommandFlags，
             * 因为这是 AOF，processCommand 中的检查无论如何都不会进行。*/
            queueMultiCommand(fakeClient, cmd->flags);
        } else {
            cmd->proc(fakeClient);
        }

        /* 伪客户端不应有回复 */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* 伪客户端绝不应该被阻塞 */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* 清理。命令代码可能已更改 argv/argc，因此我们使用客户端的
         * argv/argc 而不是局部变量。 */
        freeClientArgv(fakeClient);
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
        if (server.key_load_delay)
            debugDelay(server.key_load_delay);
    }

    /* 仅当在不出现错误的情况下到达文件结尾时才能到达此点。
     * 如果客户端处于 MULTI/EXEC 中间，即使协议在技术上是正确的，
     * 我们也将其视为短读取处理：我们希望删除未处理的尾部并继续。 */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file %s", filename);
        valid_up_to = valid_before_multi;
        goto uxeof;
    }

loaded_ok: /* 数据库已加载，清理并返回成功（AOF_OK 或 AOF_TRUNCATED）。 */
    loadingIncrProgress(ftello(fp) - last_progress_report_size);
    server.aof_state = old_aof_state;
    goto cleanup;

readerr: /* 读取错误。如果 feof(fp) 为真，则落入意外文件结尾。 */
    if (!feof(fp)) {
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file %s: %s", filename, strerror(errno));
        ret = AOF_FAILED;
        goto cleanup;
    }

uxeof: /* 意外的 AOF 文件结尾。 */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file %s!!!", filename);
        serverLog(LL_WARNING,"!!! Truncating the AOF %s at offset %llu !!!",
            filename, (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(aof_filepath,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                serverLog(LL_WARNING,"Last valid command offset is invalid");
            } else {
                serverLog(LL_WARNING,"Error truncating the AOF file %s: %s",
                    filename, strerror(errno));
            }
        } else {
            /* 确保 AOF 文件描述符在 truncate 调用之后指向文件末尾。 */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                serverLog(LL_WARNING,"Can't seek the end of the AOF file %s: %s",
                    filename, strerror(errno));
            } else {
                serverLog(LL_WARNING,
                    "AOF %s loaded anyway because aof-load-truncated is enabled", filename);
                ret = AOF_TRUNCATED;
                goto loaded_ok;
            }
        }
    }
    serverLog(LL_WARNING, "Unexpected end of file reading the append only file %s. You can: "
        "1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>. "
        "2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.", filename);
    ret = AOF_FAILED;
    goto cleanup;

fmterr: /* 格式错误。 */
    serverLog(LL_WARNING, "Bad file format reading the append only file %s: "
        "make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename);
    ret = AOF_FAILED;
    /* fall through to cleanup. */

cleanup:
    if (fakeClient) freeClient(fakeClient);
    server.current_client = old_cur_client;
    server.executing_client = old_exec_client;
    fclose(fp);
    sdsfree(aof_filepath);
    return ret;
}

/* 根据 am 指向的 aofManifest 加载 AOF 文件。 */
int loadAppendOnlyFiles(aofManifest *am) {
    serverAssert(am != NULL);
    int status, ret = AOF_OK;
    long long start;
    off_t total_size = 0, base_size = 0;
    sds aof_name;
    int total_num, aof_num = 0, last_file;

    /* 如果 'server.aof_filename' 文件存在于目录中，我们可能是从旧版 redis 启动。
     * 我们将在三种情况下进入升级模式：
     *
     * 1. 如果 'server.aof_dirname' 目录不存在
     * 2. 如果 'server.aof_dirname' 目录存在但 manifest 文件缺失
     * 3. 如果 'server.aof_dirname' 目录存在并且其中包含的 manifest 文件
     *    仅有一个 BASE AOF 记录，且此 BASE AOF 的文件名是 'server.aof_filename'，
     *    并且 'server.aof_filename' 文件不存在于 'server.aof_dirname' 目录中
     * */
    if (fileExist(server.aof_filename)) {
        if (!dirExists(server.aof_dirname) ||
            (am->base_aof_info == NULL && listLength(am->incr_aof_list) == 0) ||
            (am->base_aof_info != NULL && listLength(am->incr_aof_list) == 0 &&
             !strcmp(am->base_aof_info->file_name, server.aof_filename) && !aofFileExist(server.aof_filename)))
        {
            aofUpgradePrepare(am);
        }
    }

    if (am->base_aof_info == NULL && listLength(am->incr_aof_list) == 0) {
        return AOF_NOT_EXIST;
    }

    total_num = getBaseAndIncrAppendOnlyFilesNum(am);
    serverAssert(total_num > 0);

    /* 这里我们预先计算所有 BASE 和 INCR 文件的总大小，
     * 它将被设置为 `server.loading_total_bytes`。 */
    total_size = getBaseAndIncrAppendOnlyFilesSize(am, &status);
    if (status != AOF_OK) {
        /* 如果 manifest 中存在 AOF 但磁盘上不存在，我们认为这是一个致命错误。 */
        if (status == AOF_NOT_EXIST) status = AOF_FAILED;

        return status;
    } else if (total_size == 0) {
        return AOF_EMPTY;
    }

    startLoading(total_size, RDBFLAGS_AOF_PREAMBLE, 0);

    /* 如果需要，加载 BASE AOF。 */
    if (am->base_aof_info) {
        serverAssert(am->base_aof_info->file_type == AOF_FILE_TYPE_BASE);
        aof_name = (char*)am->base_aof_info->file_name;
        updateLoadingFileName(aof_name);
        base_size = getAppendOnlyFileSize(aof_name, NULL);
        last_file = ++aof_num == total_num;
        start = ustime();
        ret = loadSingleAppendOnlyFile(aof_name);
        if (ret == AOF_OK || (ret == AOF_TRUNCATED && last_file)) {
            serverLog(LL_NOTICE, "DB loaded from base file %s: %.3f seconds",
                aof_name, (float)(ustime()-start)/1000000);
        }

        /* 如果被截断的文件不是最后一个文件，我们认为这是一个致命错误。 */
        if (ret == AOF_TRUNCATED && !last_file) {
            ret = AOF_FAILED;
            serverLog(LL_WARNING, "Fatal error: the truncated file is not the last file");
        }

        if (ret == AOF_OPEN_ERR || ret == AOF_FAILED) {
            goto cleanup;
        }
    }

    /* 如果需要，加载 INCR AOF。 */
    if (listLength(am->incr_aof_list)) {
        listNode *ln;
        listIter li;

        listRewind(am->incr_aof_list, &li);
        while ((ln = listNext(&li)) != NULL) {
            aofInfo *ai = (aofInfo*)ln->value;
            serverAssert(ai->file_type == AOF_FILE_TYPE_INCR);
            aof_name = (char*)ai->file_name;
            updateLoadingFileName(aof_name);
            last_file = ++aof_num == total_num;
            start = ustime();
            ret = loadSingleAppendOnlyFile(aof_name);
            if (ret == AOF_OK || (ret == AOF_TRUNCATED && last_file)) {
                serverLog(LL_NOTICE, "DB loaded from incr file %s: %.3f seconds",
                    aof_name, (float)(ustime()-start)/1000000);
            }

            /* 我们知道（至少）一个 AOF 文件有数据（total_size > 0），
             * 因此空的 incr AOF 文件不计入 AOF_EMPTY 结果 */
            if (ret == AOF_EMPTY) ret = AOF_OK;

            /* 如果被截断的文件不是最后一个文件，我们认为这是一个致命错误。 */
            if (ret == AOF_TRUNCATED && !last_file) {
                ret = AOF_FAILED;
                serverLog(LL_WARNING, "Fatal error: the truncated file is not the last file");
            }

            if (ret == AOF_OPEN_ERR || ret == AOF_FAILED) {
                goto cleanup;
            }
        }
    }

    server.aof_current_size = total_size;
    /* 理想情况下，aof_rewrite_base_size 变量应保存上次重写结束时 AOF 的大小，
     * 这应包括重写期间创建的增量文件的大小，否则我们可能会面临下一次自动重写
     * 过早发生（如果 auto-aof-rewrite-percentage 较低，则立即发生）的风险。
     * 但是，由于我们未在任何地方持久化 aof_rewrite_base_size 信息，
     * 因此我们在重启时将其初始化为 BASE AOF 文件的大小。这可能会导致第一次
     * AOFRW 较早执行，但第一次 AOFRW 之后一切都会恢复正常，因此这不应成为问题。 */
    server.aof_rewrite_base_size = base_size;

cleanup:
    stopLoading(ret == AOF_OK || ret == AOF_TRUNCATED);
    return ret;
}

/* ----------------------------------------------------------------------------
 * AOF 重写
 * ------------------------------------------------------------------------- */

/* 将对象的写入委托给写入 bulk 字符串或 bulk long long。
 * 这没有放在 rio.c 中，因为那会增加对 server.h 的依赖。 */
int rioWriteBulkObject(rio *r, robj *obj) {
    /* 避免使用 getDecodedObject 以帮助写时复制（我们经常在子进程中调用此函数）。 */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rioWriteBulkLongLong(r,(long)obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r,obj->ptr,sdslen(obj->ptr));
    } else {
        serverPanic("Unknown string encoding");
    }
}

/* 发出重建列表对象所需的命令。
 * 出错时返回 0，成功时返回 1。 */
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    listTypeIterator *li = listTypeInitIterator(o,0,LIST_TAIL);
    listTypeEntry entry;
    while (listTypeNext(li,&entry)) {
        if (count == 0) {
            int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                AOF_REWRITE_ITEMS_PER_CMD : items;
            if (!rioWriteBulkCount(r,'*',2+cmd_items) ||
                !rioWriteBulkString(r,"RPUSH",5) ||
                !rioWriteBulkObject(r,key))
            {
                listTypeReleaseIterator(li);
                return 0;
            }
        }

        unsigned char *vstr;
        size_t vlen;
        long long lval;
        vstr = listTypeGetValue(&entry,&vlen,&lval);
        if (vstr) {
            if (!rioWriteBulkString(r,(char*)vstr,vlen)) {
                listTypeReleaseIterator(li);
                return 0;
            }
        } else {
            if (!rioWriteBulkLongLong(r,lval)) {
                listTypeReleaseIterator(li);
                return 0;
            }
        }
        if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }
    listTypeReleaseIterator(li);
    return 1;
}

/* 发出重建集合对象所需的命令。
 * 出错时返回 0，成功时返回 1。 */
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);
    setTypeIterator *si = setTypeInitIterator(o);
    char *str;
    size_t len;
    int64_t llval;
    while (setTypeNext(si, &str, &len, &llval) != -1) {
        if (count == 0) {
            int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                AOF_REWRITE_ITEMS_PER_CMD : items;
            if (!rioWriteBulkCount(r,'*',2+cmd_items) ||
                !rioWriteBulkString(r,"SADD",4) ||
                !rioWriteBulkObject(r,key))
            {
                setTypeReleaseIterator(si);
                return 0;
            }
        }
        size_t written = str ?
            rioWriteBulkString(r, str, len) : rioWriteBulkLongLong(r, llval);
        if (!written) {
            setTypeReleaseIterator(si);
            return 0;
        }
        if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }
    setTypeReleaseIterator(si);
    return 1;
}

/* 发出重建有序集合对象所需的命令。
 * 出错时返回 0，成功时返回 1。 */
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

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

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items*2) ||
                    !rioWriteBulkString(r,"ZADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r,score)) return 0;
            if (vstr != NULL) {
                if (!rioWriteBulkString(r,(char*)vstr,vlen)) return 0;
            } else {
                if (!rioWriteBulkLongLong(r,vll)) return 0;
            }
            zzlNext(zl,&eptr,&sptr);
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items*2) ||
                    !rioWriteBulkString(r,"ZADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    dictReleaseIterator(di);
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r,*score) ||
                !rioWriteBulkString(r,ele,sdslen(ele)))
            {
                dictReleaseIterator(di);
                return 0;
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* 写入哈希当前选中项的 key 或 value。
 * 'hi' 参数传入一个有效的 Redis 哈希迭代器。
 * 'what' 字段指定是写入 key 还是 value，可以是
 * OBJ_HASH_KEY 或 OBJ_HASH_VALUE。
 *
 * 出错时返回 0，成功时返回非零值。 */
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
    if ((hi->encoding == OBJ_ENCODING_LISTPACK) || (hi->encoding == OBJ_ENCODING_LISTPACK_EX)) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromListpack(hi, what, &vstr, &vlen, &vll, NULL);
        if (vstr)
            return rioWriteBulkString(r, (char*)vstr, vlen);
        else
            return rioWriteBulkLongLong(r, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        char *str;
        size_t len;
        hashTypeCurrentFromHashTable(hi, what, &str, &len, NULL);
        return rioWriteBulkString(r, str, len);
    }

    serverPanic("Unknown hash encoding");
    return 0;
}

/* 发出重建哈希对象所需的命令。
 * 出错时返回 0，成功时返回 1。 */
int rewriteHashObject(rio *r, robj *key, robj *o) {
    int res = 0; /*fail*/

    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o, 0);

    int isHFE = hashTypeGetMinExpire(o, 0) != EB_EXPIRE_TIME_INVALID;
    hi = hashTypeInitIterator(o);

    if (!isHFE) {
        while (hashTypeNext(hi, 0) != C_ERR) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;
                if (!rioWriteBulkCount(r, '*', 2 + cmd_items * 2) ||
                    !rioWriteBulkString(r, "HMSET", 5) ||
                    !rioWriteBulkObject(r, key))
                    goto reHashEnd;
            }

            if (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY) ||
                !rioWriteHashIteratorCursor(r, hi, OBJ_HASH_VALUE))
                goto reHashEnd;

            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else {
        while (hashTypeNext(hi, 0) != C_ERR) {

            char hmsetCmd[] = "*4\r\n$5\r\nHMSET\r\n";
            if ( (!rioWrite(r, hmsetCmd, sizeof(hmsetCmd) - 1)) ||
                 (!rioWriteBulkObject(r, key)) ||
                 (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY)) ||
                 (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_VALUE)) )
                goto reHashEnd;

            if (hi->expire_time != EB_EXPIRE_TIME_INVALID) {
                char cmd[] = "*6\r\n$10\r\nHPEXPIREAT\r\n";
                if ( (!rioWrite(r, cmd, sizeof(cmd) - 1)) ||
                     (!rioWriteBulkObject(r, key)) ||
                     (!rioWriteBulkLongLong(r, hi->expire_time)) ||
                     (!rioWriteBulkString(r, "FIELDS", 6)) ||
                     (!rioWriteBulkString(r, "1", 1)) ||
                     (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY)) )
                    goto reHashEnd;
            }
        }
    }

    res = 1; /* success */

reHashEnd:
    hashTypeReleaseIterator(hi);
    return res;
}

/* rewriteStreamObject() 的辅助函数，在 AOF 中生成表示 ID 'id' 的 bulk 字符串。 */
int rioWriteBulkStreamID(rio *r,streamID *id) {
    int retval;

    sds replyid = sdscatfmt(sdsempty(),"%U-%U",id->ms,id->seq);
    retval = rioWriteBulkString(r,replyid,sdslen(replyid));
    sdsfree(replyid);
    return retval;
}

/* rewriteStreamObject() 的辅助函数：发出 XCLAIM 命令以将
 * 由 'nack' 描述的 ID 为 'rawid' 的消息添加到指定消费者的待处理列表中。
 * 所有这一切都在指定 key 和 group 的上下文中进行。 */
int rioWriteStreamPendingEntry(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer, unsigned char *rawid, streamNACK *nack) {
     /* XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
               RETRYCOUNT <count> JUSTID FORCE. */
    streamID id;
    streamDecodeID(rawid,&id);
    if (rioWriteBulkCount(r,'*',12) == 0) return 0;
    if (rioWriteBulkString(r,"XCLAIM",6) == 0) return 0;
    if (rioWriteBulkObject(r,key) == 0) return 0;
    if (rioWriteBulkString(r,groupname,groupname_len) == 0) return 0;
    if (rioWriteBulkString(r,consumer->name,sdslen(consumer->name)) == 0) return 0;
    if (rioWriteBulkString(r,"0",1) == 0) return 0;
    if (rioWriteBulkStreamID(r,&id) == 0) return 0;
    if (rioWriteBulkString(r,"TIME",4) == 0) return 0;
    if (rioWriteBulkLongLong(r,nack->delivery_time) == 0) return 0;
    if (rioWriteBulkString(r,"RETRYCOUNT",10) == 0) return 0;
    if (rioWriteBulkLongLong(r,nack->delivery_count) == 0) return 0;
    if (rioWriteBulkString(r,"JUSTID",6) == 0) return 0;
    if (rioWriteBulkString(r,"FORCE",5) == 0) return 0;
    return 1;
}

/* rewriteStreamObject() 的辅助函数：如果需要，发出 XGROUP CREATECONSUMER
 * 以创建没有任何待处理条目的消费者。
 * 所有这一切都在指定 key 和 group 的上下文中进行。 */
int rioWriteStreamEmptyConsumer(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer) {
    /* XGROUP CREATECONSUMER <key> <group> <consumer> */
    if (rioWriteBulkCount(r,'*',5) == 0) return 0;
    if (rioWriteBulkString(r,"XGROUP",6) == 0) return 0;
    if (rioWriteBulkString(r,"CREATECONSUMER",14) == 0) return 0;
    if (rioWriteBulkObject(r,key) == 0) return 0;
    if (rioWriteBulkString(r,groupname,groupname_len) == 0) return 0;
    if (rioWriteBulkString(r,consumer->name,sdslen(consumer->name)) == 0) return 0;
    return 1;
}

/* 发出重建流对象所需的命令。
 * 出错时返回 0，成功时返回 1。 */
int rewriteStreamObject(rio *r, robj *key, robj *o) {
    stream *s = o->ptr;
    streamIterator si;
    streamIteratorStart(&si,s,NULL,NULL,0);
    streamID id;
    int64_t numfields;

    if (s->length) {
        /* 使用 XADD 命令重建流数据。 */
        while(streamIteratorGetID(&si,&id,&numfields)) {
            /* 为每个条目发出一个两元素数组。第一个是 ID，
             * 第二个是字段-值对数组。 */

            /* 发出 XADD <key> <id> ...fields... 命令。 */
            if (!rioWriteBulkCount(r,'*',3+numfields*2) || 
                !rioWriteBulkString(r,"XADD",4) ||
                !rioWriteBulkObject(r,key) ||
                !rioWriteBulkStreamID(r,&id)) 
            {
                streamIteratorStop(&si);
                return 0;
            }
            while(numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si,&field,&value,&field_len,&value_len);
                if (!rioWriteBulkString(r,(char*)field,field_len) ||
                    !rioWriteBulkString(r,(char*)value,value_len)) 
                {
                    streamIteratorStop(&si);
                    return 0;                  
                }
            }
        }
    } else {
        /* 如果我们序列化的 key 是空字符串（这在 Stream 类型中是可能的），
         * 使用 XADD MAXLEN 0 技巧来生成一个空流。 */
        id.ms = 0; id.seq = 1; 
        if (!rioWriteBulkCount(r,'*',7) ||
            !rioWriteBulkString(r,"XADD",4) ||
            !rioWriteBulkObject(r,key) ||
            !rioWriteBulkString(r,"MAXLEN",6) ||
            !rioWriteBulkString(r,"0",1) ||
            !rioWriteBulkStreamID(r,&id) ||
            !rioWriteBulkString(r,"x",1) ||
            !rioWriteBulkString(r,"y",1))
        {
            streamIteratorStop(&si);
            return 0;     
        }
    }

    /* 在 XADD 之后追加 XSETID，确保 lastid 正确，
     * 以处理 XDEL 导致的 lastid 变化。 */
    if (!rioWriteBulkCount(r,'*',7) ||
        !rioWriteBulkString(r,"XSETID",6) ||
        !rioWriteBulkObject(r,key) ||
        !rioWriteBulkStreamID(r,&s->last_id) ||
        !rioWriteBulkString(r,"ENTRIESADDED",12) ||
        !rioWriteBulkLongLong(r,s->entries_added) ||
        !rioWriteBulkString(r,"MAXDELETEDID",12) ||
        !rioWriteBulkStreamID(r,&s->max_deleted_entry_id)) 
    {
        streamIteratorStop(&si);
        return 0; 
    }


    /* 创建所有的流消费者组。 */
    if (s->cgroups) {
        raxIterator ri;
        raxStart(&ri,s->cgroups);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            streamCG *group = ri.data;
            /* 发出 XGROUP CREATE 命令以创建消费者组。 */
            if (!rioWriteBulkCount(r,'*',7) ||
                !rioWriteBulkString(r,"XGROUP",6) ||
                !rioWriteBulkString(r,"CREATE",6) ||
                !rioWriteBulkObject(r,key) ||
                !rioWriteBulkString(r,(char*)ri.key,ri.key_len) ||
                !rioWriteBulkStreamID(r,&group->last_id) ||
                !rioWriteBulkString(r,"ENTRIESREAD",11) ||
                !rioWriteBulkLongLong(r,group->entries_read))
            {
                raxStop(&ri);
                streamIteratorStop(&si);
                return 0;
            }

            /* 为每个有待处理条目的消费者生成 XCLAIM 命令。
             * 没有待处理条目的消费者将通过
             * XGROUP CREATECONSUMER 生成。 */
            raxIterator ri_cons;
            raxStart(&ri_cons,group->consumers);
            raxSeek(&ri_cons,"^",NULL,0);
            while(raxNext(&ri_cons)) {
                streamConsumer *consumer = ri_cons.data;
                /* 如果没有待处理条目，仅发出 XGROUP CREATECONSUMER */
                if (raxSize(consumer->pel) == 0) {
                    if (rioWriteStreamEmptyConsumer(r,key,(char*)ri.key,
                                                    ri.key_len,consumer) == 0)
                    {
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                    continue;
                }
                /* 对于当前消费者，遍历所有 PEL 条目
                 * 以发出 XCLAIM 协议。 */
                raxIterator ri_pel;
                raxStart(&ri_pel,consumer->pel);
                raxSeek(&ri_pel,"^",NULL,0);
                while(raxNext(&ri_pel)) {
                    streamNACK *nack = ri_pel.data;
                    if (rioWriteStreamPendingEntry(r,key,(char*)ri.key,
                                                   ri.key_len,consumer,
                                                   ri_pel.key,nack) == 0)
                    {
                        raxStop(&ri_pel);
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                }
                raxStop(&ri_pel);
            }
            raxStop(&ri_cons);
        }
        raxStop(&ri);
    }

    streamIteratorStop(&si);
    return 1;
}

/* 调用模块类型回调以重写由模块导出且 Redis 本身不处理的数据类型。
 * 出错时返回 0，成功时返回 1。 */
int rewriteModuleObject(rio *r, robj *key, robj *o, int dbid) {
    RedisModuleIO io;
    moduleValue *mv = o->ptr;
    moduleType *mt = mv->type;
    moduleInitIOContext(io,mt,r,key,dbid);
    mt->aof_rewrite(&io,key,mv->value);
    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    return io.error ? 0 : 1;
}

/* 将所有已加载的函数库写入到 AOF。 */
static int rewriteFunctions(rio *aof) {
    dict *functions = functionsLibGet();
    dictIterator *iter = dictGetIterator(functions);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        if (rioWrite(aof, "*3\r\n", 4) == 0) goto werr;
        char function_load[] = "$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n";
        if (rioWrite(aof, function_load, sizeof(function_load) - 1) == 0) goto werr;
        if (rioWriteBulkString(aof, li->code, sdslen(li->code)) == 0) goto werr;
    }
    dictReleaseIterator(iter);
    return 1;

werr:
    dictReleaseIterator(iter);
    return 0;
}

/* 将数据库的内容以 AOF 命令序列的形式写入到 rio 中。
 * 这是 rewriteAppendOnlyFile 中实际执行 AOF 重写的核心函数。
 * 出错时返回 C_ERR，否则返回 C_OK。 */
int rewriteAppendOnlyFileRio(rio *aof) {
    dictEntry *de;
    int j;
    long key_count = 0;
    long long updated_time = 0;
    kvstoreIterator *kvs_it = NULL;

    /* 在重写 AOF 开始时记录时间戳。 */
    if (server.aof_timestamp_enabled) {
        sds ts = genAofTimestampAnnotationIfNeeded(1);
        if (rioWrite(aof,ts,sdslen(ts)) == 0) { sdsfree(ts); goto werr; }
        sdsfree(ts);
    }

    if (rewriteFunctions(aof) == 0) goto werr;

    for (j = 0; j < server.dbnum; j++) {
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db + j;
        if (kvstoreSize(db->keys) == 0) continue;

        /* 选择新的数据库 */
        if (rioWrite(aof,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(aof,j) == 0) goto werr;

        kvs_it = kvstoreIteratorInit(db->keys);
        /* 迭代该数据库并写入每个条目 */
        while((de = kvstoreIteratorNext(kvs_it)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;
            size_t aof_bytes_before_key = aof->processed_bytes;

            keystr = dictGetKey(de);
            o = dictGetVal(de);
            initStaticStringObject(key,keystr);

            expiretime = getExpire(db,&key);

            /* 保存 key 和关联的值 */
            if (o->type == OBJ_STRING) {
                /* 发出 SET 命令 */
                char cmd[]="*3\r\n$3\r\nSET\r\n";
                if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                /* key 和 value */
                if (rioWriteBulkObject(aof,&key) == 0) goto werr;
                if (rioWriteBulkObject(aof,o) == 0) goto werr;
            } else if (o->type == OBJ_LIST) {
                if (rewriteListObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_SET) {
                if (rewriteSetObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_ZSET) {
                if (rewriteSortedSetObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_HASH) {
                if (rewriteHashObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_STREAM) {
                if (rewriteStreamObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_MODULE) {
                if (rewriteModuleObject(aof,&key,o,j) == 0) goto werr;
            } else {
                serverPanic("Unknown object type");
            }

            /* 在 fork 子进程中，我们可以尝试将内存释放回 OS，
             * 并可能避免或减少 COW。我们向 dismiss 机制提供一个关于
             * 已存储对象估计大小的提示。 */
            size_t dump_size = aof->processed_bytes - aof_bytes_before_key;
            if (server.in_fork_child) dismissObject(o, dump_size);

            /* 保存过期时间 */
            if (expiretime != -1) {
                char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";
                if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                if (rioWriteBulkObject(aof,&key) == 0) goto werr;
                if (rioWriteBulkLongLong(aof,expiretime) == 0) goto werr;
            }

            /* 大约每 1 秒更新一次信息。
             * 为了避免在每次迭代中调用 mstime()，我们将每 1024 个 key 检查一次差异 */
            if ((key_count++ & 1023) == 0) {
                long long now = mstime();
                if (now - updated_time >= 1000) {
                    sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
                    updated_time = now;
                }
            }

            /* 如果需要（在测试中），在处理下一个 key 之前延迟 */
            if (server.rdb_key_save_delay)
                debugDelay(server.rdb_key_save_delay);
        }
        kvstoreIteratorRelease(kvs_it);
    }
    return C_OK;

werr:
    if (kvs_it) kvstoreIteratorRelease(kvs_it);
    return C_ERR;
}

/* 将能够完全重建数据集的命令序列写入 "filename"。REWRITEAOF 和 BGREWRITEAOF 都使用此函数。
 *
 * 为了最小化重写日志中所需的命令数，Redis 在可能的情况下使用可变参数命令，
 * 例如 RPUSH、SADD 和 ZADD。但是每次最多使用 AOF_REWRITE_ITEMS_PER_CMD 个
 * 项目通过单个命令插入。 */
int rewriteAppendOnlyFile(char *filename) {
    rio aof;
    FILE *fp = NULL;
    char tmpfile[256];

    /* 注意，此处使用的临时文件名必须与
     * rewriteAppendOnlyFileBackground() 函数中使用的不同。 */
    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        serverLog(LL_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return C_ERR;
    }

    rioInitWithFile(&aof,fp);

    if (server.aof_rewrite_incremental_fsync) {
        rioSetAutoSync(&aof,REDIS_AUTOSYNC_BYTES);
        rioSetReclaimCache(&aof,1);
    }

    startSaving(RDBFLAGS_AOF_PREAMBLE);

    if (server.aof_use_rdb_preamble) {
        int error;
        if (rdbSaveRio(SLAVE_REQ_NONE,&aof,&error,RDBFLAGS_AOF_PREAMBLE,NULL) == C_ERR) {
            errno = error;
            goto werr;
        }
    } else {
        if (rewriteAppendOnlyFileRio(&aof) == C_ERR) goto werr;
    }

    /* 确保数据不会残留在操作系统的输出缓冲区中 */
    if (fflush(fp)) goto werr;
    if (fsync(fileno(fp))) goto werr;
    if (reclaimFilePageCache(fileno(fp), 0, 0) == -1) {
        /* 小错误，仅记录日志以了解情况 */
        serverLog(LL_NOTICE,"Unable to reclaim page cache: %s", strerror(errno));
    }
    if (fclose(fp)) { fp = NULL; goto werr; }
    fp = NULL;

    /* 使用 RENAME 确保仅在生成的文件正确时才原子性地更改 DB 文件。 */
    if (rename(tmpfile,filename) == -1) {
        serverLog(LL_WARNING,"Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    stopSaving(1);

    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error writing append only file on disk: %s", strerror(errno));
    if (fp) fclose(fp);
    unlink(tmpfile);
    stopSaving(0);
    return C_ERR;
}
/* ----------------------------------------------------------------------------
 * AOF 后台重写
 * ------------------------------------------------------------------------- */

/* 以下是 AOF 文件后台重写的工作流程：
 *
 * 1) 用户调用 BGREWRITEAOF 命令
 * 2) Redis 调用此函数，该函数执行 fork()：
 *    2a) 子进程将 AOF 文件重写到临时文件中。
 *    2b) 父进程打开一个新的 INCR AOF 文件以继续写入。
 * 3) 当子进程完成 '2a' 后退出。
 * 4) 父进程将捕获退出码，如果成功，它将：
 *    4a) 获取新的 BASE 文件名，并将之前的（如果有）标记为 HISTORY 类型
 *    4b) 使用 rename(2) 将临时文件重命名为新的 BASE 文件名
 *    4c) 将已重写的 INCR AOF 标记为 history 类型
 *    4d) 持久化 AOF manifest 文件
 *    4e) 使用 bio 线程删除 history 文件
 */
int rewriteAppendOnlyFileBackground(void) {
    pid_t childpid;

    if (hasActiveChildProcess()) return C_ERR;

    if (dirCreateIfMissing(server.aof_dirname) == -1) {
        serverLog(LL_WARNING, "Can't open or create append-only dir %s: %s",
            server.aof_dirname, strerror(errno));
        server.aof_lastbgrewrite_status = C_ERR;
        return C_ERR;
    }

    /* 将 aof_selected_db 设为 -1，以强制下一次调用
     * feedAppendOnlyFile() 时发出 SELECT 命令。 */
    server.aof_selected_db = -1;
    flushAppendOnlyFile(1);
    if (openNewIncrAofForAppend() != C_OK) {
        server.aof_lastbgrewrite_status = C_ERR;
        return C_ERR;
    }

    if (server.aof_state == AOF_WAIT_REWRITE) {
        /* 等待所有与 AOF 相关的 bio 任务完成。这可以防止属于
         * 前一个 AOF 的工作线程的 `fsynced_reploff_pending` 更新
         * 与新 AOF 之间的竞态条件。此问题特指全量同步场景，
         * 我们不希望在切换到不同主节点时，已确认的复制偏移
         * 发生向前或向后的跳跃。 */
        bioDrainWorker(BIO_AOF_FSYNC);

        /* 设置初始 repl_offset，它将在 AOFRW 完成时
         * （可能已被 bio 线程更新后）应用于 fsynced_reploff */
        atomicSet(server.fsynced_reploff_pending, server.master_repl_offset);
        server.fsynced_reploff = 0;
    }

    server.stat_aof_rewrites++;

    if ((childpid = redisFork(CHILD_TYPE_AOF)) == 0) {
        char tmpfile[256];

        /* 子进程 */
        redisSetProcTitle("redis-aof-rewrite");
        redisSetCpuAffinity(server.aof_rewrite_cpulist);
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) getpid());
        if (rewriteAppendOnlyFile(tmpfile) == C_OK) {
            serverLog(LL_NOTICE,
                "Successfully created the temporary AOF base file %s", tmpfile);
            sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE, "AOF rewrite");
            exitFromChild(0);
        } else {
            exitFromChild(1);
        }
    } else {
        /* 父进程 */
        if (childpid == -1) {
            server.aof_lastbgrewrite_status = C_ERR;
            serverLog(LL_WARNING,
                "Can't rewrite append only file in background: fork: %s",
                strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,
            "Background append only file rewriting started by pid %ld",(long) childpid);
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        return C_OK;
    }
    return C_OK; /* unreached */
}

/* BGREWRITEAOF 命令的处理函数。
 * 手动触发 AOF 后台重写。 */
void bgrewriteaofCommand(client *c) {
    if (server.child_type == CHILD_TYPE_AOF) {
        addReplyError(c,"Background append only file rewriting already in progress");
    } else if (hasActiveChildProcess() || server.in_exec) {
        server.aof_rewrite_scheduled = 1;
        /* 手动触发 AOFRW 时重置失败计数，
         * 以便可以立即执行。 */
        server.stat_aofrw_consecutive_failures = 0;
        addReplyStatus(c,"Background append only file rewriting scheduled");
    } else if (rewriteAppendOnlyFileBackground() == C_OK) {
        addReplyStatus(c,"Background append only file rewriting started");
    } else {
        addReplyError(c,"Can't execute an AOF background rewriting. "
                        "Please check the server logs for more information.");
    }
}

/* 移除 AOF 重写过程中产生的临时文件。
 * 包括后台重写和普通重写的临时文件。 */
void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) childpid);
    bg_unlink(tmpfile);

    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) childpid);
    bg_unlink(tmpfile);
}

/* 获取 AOF 文件的大小。
 * status 参数是可选的输出参数，用于填充
 * AOF_ 状态值之一。 */
off_t getAppendOnlyFileSize(sds filename, int *status) {
    struct redis_stat sb;
    off_t size;
    mstime_t latency;

    sds aof_filepath = makePath(server.aof_dirname, filename);
    latencyStartMonitor(latency);
    if (redis_stat(aof_filepath, &sb) == -1) {
        if (status) *status = errno == ENOENT ? AOF_NOT_EXIST : AOF_OPEN_ERR;
        serverLog(LL_WARNING, "Unable to obtain the AOF file %s length. stat: %s",
            filename, strerror(errno));
        size = 0;
    } else {
        if (status) *status = AOF_OK;
        size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat", latency);
    sdsfree(aof_filepath);
    return size;
}

/* 获取 manifest 中引用的所有 AOF 文件的大小（不包括 history 文件）。
 * status 参数是输出参数，用于填充
 * AOF_ 状态值之一。 */
off_t getBaseAndIncrAppendOnlyFilesSize(aofManifest *am, int *status) {
    off_t size = 0;
    listNode *ln;
    listIter li;

    if (am->base_aof_info) {
        serverAssert(am->base_aof_info->file_type == AOF_FILE_TYPE_BASE);

        size += getAppendOnlyFileSize(am->base_aof_info->file_name, status);
        if (*status != AOF_OK) return 0;
    }

    listRewind(am->incr_aof_list, &li);
    while ((ln = listNext(&li)) != NULL) {
        aofInfo *ai = (aofInfo*)ln->value;
        serverAssert(ai->file_type == AOF_FILE_TYPE_INCR);
        size += getAppendOnlyFileSize(ai->file_name, status);
        if (*status != AOF_OK) return 0;
    }

    return size;
}

/* 获取 manifest 中 BASE 和 INCR AOF 文件的总数。 */
int getBaseAndIncrAppendOnlyFilesNum(aofManifest *am) {
    int num = 0;
    if (am->base_aof_info) num++;
    if (am->incr_aof_list) num += listLength(am->incr_aof_list);
    return num;
}

/* 后台 AOF 文件重写（BGREWRITEAOF）完成后的处理函数。
 * 根据子进程的退出状态执行相应的清理和更新操作。 */
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        char tmpfile[256];
        long long now = ustime();
        sds new_base_filepath = NULL;
        sds new_incr_filepath = NULL;
        aofManifest *temp_am;
        mstime_t latency;

        serverLog(LL_NOTICE,
            "Background AOF rewrite terminated with success");

        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof",
            (int)server.child_pid);

        serverAssert(server.aof_manifest != NULL);

        /* 复制一个临时的 aof_manifest 以进行后续修改。 */
        temp_am = aofManifestDup(server.aof_manifest);

        /* 获取新的 BASE 文件名，并将之前的（如果有）
         * 标记为 HISTORY 类型。 */
        sds new_base_filename = getNewBaseFileNameAndMarkPreAsHistory(temp_am);
        serverAssert(new_base_filename != NULL);
        new_base_filepath = makePath(server.aof_dirname, new_base_filename);

        /* 将临时 AOF 文件重命名为 'new_base_filename'。 */
        latencyStartMonitor(latency);
        if (rename(tmpfile, new_base_filepath) == -1) {
            serverLog(LL_WARNING,
                "Error trying to rename the temporary AOF base file %s into %s: %s",
                tmpfile,
                new_base_filepath,
                strerror(errno));
            aofManifestFree(temp_am);
            sdsfree(new_base_filepath);
            server.aof_lastbgrewrite_status = C_ERR;
            server.stat_aofrw_consecutive_failures++;
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rename", latency);
        serverLog(LL_NOTICE,
            "Successfully renamed the temporary AOF base file %s into %s", tmpfile, new_base_filename);

        /* 将临时 incr AOF 文件重命名为 'new_incr_filename'。 */
        if (server.aof_state == AOF_WAIT_REWRITE) {
            /* 获取临时 incr AOF 名称。 */
            sds temp_incr_aof_name = getTempIncrAofName();
            sds temp_incr_filepath = makePath(server.aof_dirname, temp_incr_aof_name);
            /* 获取下一个新的 incr AOF 名称。 */
            sds new_incr_filename = getNewIncrAofName(temp_am);
            new_incr_filepath = makePath(server.aof_dirname, new_incr_filename);
            latencyStartMonitor(latency);
            if (rename(temp_incr_filepath, new_incr_filepath) == -1) {
                serverLog(LL_WARNING,
                    "Error trying to rename the temporary AOF incr file %s into %s: %s",
                    temp_incr_filepath,
                    new_incr_filepath,
                    strerror(errno));
                bg_unlink(new_base_filepath);
                sdsfree(new_base_filepath);
                aofManifestFree(temp_am);
                sdsfree(temp_incr_filepath);
                sdsfree(new_incr_filepath);
                sdsfree(temp_incr_aof_name);
                server.aof_lastbgrewrite_status = C_ERR;
                server.stat_aofrw_consecutive_failures++;
                goto cleanup;
            }
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("aof-rename", latency);
            serverLog(LL_NOTICE,
                "Successfully renamed the temporary AOF incr file %s into %s", temp_incr_aof_name, new_incr_filename);
            sdsfree(temp_incr_filepath);
            sdsfree(temp_incr_aof_name);
        }

        /* 将 'incr_aof_list' 中的 AOF 文件类型从 AOF_FILE_TYPE_INCR
         * 更改为 AOF_FILE_TYPE_HIST，并将它们移至 'history_aof_list'。 */
        markRewrittenIncrAofAsHistory(temp_am);

        /* 持久化我们的修改。 */
        if (persistAofManifest(temp_am) == C_ERR) {
            bg_unlink(new_base_filepath);
            aofManifestFree(temp_am);
            sdsfree(new_base_filepath);
            if (new_incr_filepath) {
                bg_unlink(new_incr_filepath);
                sdsfree(new_incr_filepath);
            }
            server.aof_lastbgrewrite_status = C_ERR;
            server.stat_aofrw_consecutive_failures++;
            goto cleanup;
        }
        sdsfree(new_base_filepath);
        if (new_incr_filepath) sdsfree(new_incr_filepath);

        /* 我们可以安全地让 `server.aof_manifest` 指向 'temp_am' 并释放之前的那个。 */
        aofManifestFreeAndUpdate(temp_am);

        if (server.aof_state != AOF_OFF) {
            /* AOF 已启用。 */
            server.aof_current_size = getAppendOnlyFileSize(new_base_filename, NULL) + server.aof_last_incr_size;
            server.aof_rewrite_base_size = server.aof_current_size;
        }

        /* 我们不关心 `aofDelHistoryFiles` 的返回值，因为 history
         * 文件删除失败不会导致任何问题。 */
        aofDelHistoryFiles();

        server.aof_lastbgrewrite_status = C_OK;
        server.stat_aofrw_consecutive_failures = 0;

        serverLog(LL_NOTICE, "Background AOF rewrite finished successfully");
        /* 如果需要，将状态从 WAIT_REWRITE 更改为 ON */
        if (server.aof_state == AOF_WAIT_REWRITE) {
            server.aof_state = AOF_ON;

            /* 更新刚刚变为有效的 fsync 复制偏移。
             * 这可能是我们在 startAppendOnly 中获取的，
             * 也可能是由 bio 线程设置的更新值。 */
            long long fsynced_reploff_pending;
            atomicGet(server.fsynced_reploff_pending, fsynced_reploff_pending);
            server.fsynced_reploff = fsynced_reploff_pending;
        }

        serverLog(LL_VERBOSE,
            "Background AOF rewrite signal handler took %lldus", ustime()-now);
    } else if (!bysignal && exitcode != 0) {
        server.aof_lastbgrewrite_status = C_ERR;
        server.stat_aofrw_consecutive_failures++;

        serverLog(LL_WARNING,
            "Background AOF rewrite terminated with error");
    } else {
        /* SIGUSR1 在白名单中，因此我们可以终止子进程
         * 而不会触发错误条件。 */
        if (bysignal != SIGUSR1) {
            server.aof_lastbgrewrite_status = C_ERR;
            server.stat_aofrw_consecutive_failures++;
        }

        serverLog(LL_WARNING,
            "Background AOF rewrite terminated by signal %d", bysignal);
    }

cleanup:
    aofRemoveTempFile(server.child_pid);
    /* 清空 AOF 缓冲区并删除临时 incr AOF 以便下次重写。 */
    if (server.aof_state == AOF_WAIT_REWRITE) {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
        aofDelTempIncrAofFile();
    }
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* 如果我们正在等待重写以切换 AOF 为启用状态，则安排新的重写。 */
    if (server.aof_state == AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}
