/* rio.c 是一个简单的面向流的 I/O 抽象层，
 * 它提供了一套接口，使得上层代码可以使用不同的具体输入/输出设备
 * 来消费/产生数据。例如，同一份 rdb.c 中的代码借助 rio 抽象层，
 * 既可以使用内存缓冲区，也可以使用文件来读写 RDB 格式的数据。
 *
 * 一个 rio 对象提供以下方法：
 *  read：从流中读取数据。
 *  write：向流中写入数据。
 *  tell：获取当前偏移量。
 *
 * 同时还可以设置一个 'checksum'（校验和）方法，rio.c 用它来
 * 计算已写入或已读取数据的校验和，或查询 rio 对象当前的校验和。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-current, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "fmacros.h"
#include "fpconv_dtoa.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"

/* ------------------------- 缓冲区 I/O 实现 ----------------------- */

/* 缓冲区 I/O 后端：将数据追加到 sds 缓冲区中。
 * 返回值：成功返回 1，失败返回 0。 */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    // 使用 sdscatlen 将数据追加到 sds 缓冲区，并更新写入位置
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* 缓冲区 I/O 后端：从 sds 缓冲区中读取数据。
 * 返回值：成功返回 1，失败返回 0。 */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    // 若缓冲区中剩余的数据不足 len 字节，则读取失败
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* 缓冲区中数据不足，无法返回 len 字节。 */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* 返回缓冲区中的当前读写位置。 */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* 将缓冲区中的数据刷新到目标设备（如适用）。
 * 返回值：成功返回 1，失败返回 0。 */
static int rioBufferFlush(rio *r) {
    UNUSED(r);
    return 1; /* 无需做任何事，写操作只是将数据追加到缓冲区。 */
}

/* rioBufferIO：基于 sds 缓冲区的 rio 实现模板。
 * 该结构体被复制到每个 rioInitWithBuffer 初始化的 rio 对象中。 */
static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,           /* update_checksum：默认无校验和 */
    0,              /* 当前校验和 */
    0,              /* flags：标志位 */
    0,              /* 已读取或已写入的字节数 */
    0,              /* 单次读写块大小 */
    { { NULL, 0 } } /* io-specific 变量联合体 */
};

/* 使用给定的 sds 字符串初始化一个基于缓冲区的 rio 流。
 * 调用后，r 将指向传入的 sds，可以直接对其进行读写操作。 */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- 标准 I/O 文件指针实现 ------------------- */

/* 文件 I/O 后端：将数据写入到 FILE* 指向的文件。
 * 如果开启了 autosync，则按 autosync 阈值分批写入并异步刷盘。
 * 返回值：成功返回 1，失败返回 0。 */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    // 未开启 autosync 时直接调用标准 fwrite
    if (!r->io.file.autosync) return fwrite(buf,len,1,r->io.file.fp);

    size_t nwritten = 0;
    /* 增量地将数据写入文件，避免单次写入超过 autosync 阈值，
     * 这样内核缓冲区缓存中就不会一次性积累过多的脏页。 */
    while (len != nwritten) {
        serverAssert(r->io.file.autosync > r->io.file.buffered);
        // 计算本轮最多可写入的字节数（对齐到 autosync 阈值）
        size_t nalign = (size_t)(r->io.file.autosync - r->io.file.buffered);
        size_t towrite = nalign > len-nwritten ? len-nwritten : nalign;

        if (fwrite((char*)buf+nwritten,towrite,1,r->io.file.fp) == 0) return 0;
        nwritten += towrite;
        r->io.file.buffered += towrite;

        // 当缓冲的字节数达到 autosync 阈值时触发刷盘
        if (r->io.file.buffered >= r->io.file.autosync) {
            fflush(r->io.file.fp);

            size_t processed = r->processed_bytes + nwritten;
            serverAssert(processed % r->io.file.autosync == 0);
            serverAssert(r->io.file.buffered == r->io.file.autosync);

#if HAVE_SYNC_FILE_RANGE
            /* 异步发起写回请求。 */
            if (sync_file_range(fileno(r->io.file.fp),
                    processed - r->io.file.autosync, r->io.file.autosync,
                    SYNC_FILE_RANGE_WRITE) == -1)
                return 0;

            if (processed >= (size_t)r->io.file.autosync * 2) {
                /* 为了兑现 'autosync' 的承诺，需要确保上一次
                 * 异步写回的数据已落盘。如果上一次写回尚未完成
                 * （磁盘较慢），此次调用可能会阻塞。 */
                if (sync_file_range(fileno(r->io.file.fp),
                        processed - r->io.file.autosync*2,
                        r->io.file.autosync, SYNC_FILE_RANGE_WAIT_BEFORE|
                        SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER) == -1)
                    return 0;
            }
#else
            /* 没有 sync_file_range 时退化为同步 fsync。 */
            if (redis_fsync(fileno(r->io.file.fp)) == -1) return 0;
#endif
            if (r->io.file.reclaim_cache) {
                /* 在 Linux 上 sync_file_range 仅向操作系统发起写回请求，
                 * 当调用 posix_fadvise 时，脏页可能还在刷盘中，
                 * 这意味着它会被 posix_fadvise 忽略。
                 *
                 * 因此我们对整个文件调用 posix_fadvise，已经写回的页
                 * 在后续还有机会被回收。 */
                reclaimFilePageCache(fileno(r->io.file.fp), 0, 0);
            }
            r->io.file.buffered = 0;
        }
    }
    return 1;
}

/* 文件 I/O 后端：从 FILE* 指向的文件中读取数据。
 * 返回值：成功返回 1，失败返回 0。 */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* 返回文件中的当前读写位置。 */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* 将文件缓冲区中的数据刷新到磁盘（如适用）。
 * 返回值：成功返回 1，失败返回 0。 */
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

/* rioFileIO：基于 stdio FILE* 的 rio 实现模板。 */
static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,           /* update_checksum：默认无校验和 */
    0,              /* 当前校验和 */
    0,              /* flags：标志位 */
    0,              /* 已读取或已写入的字节数 */
    0,              /* 单次读写块大小 */
    { { NULL, 0 } } /* io-specific 变量联合体 */
};

/* 使用给定的 FILE* 初始化一个基于文件的 rio 流。
 * 初始化时 autosync 默认关闭，reclaim_cache 默认关闭。 */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
    r->io.file.reclaim_cache = 0;
}

/* ------------------- 连接实现 -------------------
 * 该 RIO 实现用于通过 rdbLoadRio() 直接从连接中读取 RDB 文件，
 * 因此它只实现了从连接（通常是一个 socket）读取数据。 */

static size_t rioConnWrite(rio *r, const void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* 错误：此目标后端尚未支持写入操作。 */
}

/* 连接 I/O 后端：从底层 connection 中读取数据。
 * 返回值：成功返回实际读取的字节数（等于 len），失败返回 0。 */
static size_t rioConnRead(rio *r, void *buf, size_t len) {
    // 当前缓冲区中尚未消费的可用字节数
    size_t avail = sdslen(r->io.conn.buf)-r->io.conn.pos;

    /* 如果缓冲区太小无法容纳整个请求：扩容。 */
    if (sdslen(r->io.conn.buf) + sdsavail(r->io.conn.buf) < len)
        r->io.conn.buf = sdsMakeRoomFor(r->io.conn.buf, len - sdslen(r->io.conn.buf));

    /* 如果缓冲区剩余空闲空间仍不足：通过 memmove 将已读部分移走，
     * 为后续读取腾出空间。 */
    if (len > avail && sdsavail(r->io.conn.buf) < len - avail) {
        sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        r->io.conn.pos = 0;
    }

    /* 确保调用者没有请求越过 read_limit 进行读取。
     * 若未越界则继续读到上限；若越界则返回错误。 */
    if (r->io.conn.read_limit != 0 && r->io.conn.read_limit < r->io.conn.read_so_far + len) {
        errno = EOVERFLOW;
        return 0;
    }

    /* 如果 sds 中还没有足够的数据，则从连接中继续读取。 */
    while (len > sdslen(r->io.conn.buf) - r->io.conn.pos) {
        size_t buffered = sdslen(r->io.conn.buf) - r->io.conn.pos;
        size_t needs = len - buffered;
        /* 读取量为「仍需的字节数」或 PROTO_IOBUF_LEN 二者中较大的一个。 */
        size_t toread = needs < PROTO_IOBUF_LEN ? PROTO_IOBUF_LEN: needs;
        if (toread > sdsavail(r->io.conn.buf)) toread = sdsavail(r->io.conn.buf);
        // 不能超过 read_limit 上限
        if (r->io.conn.read_limit != 0 &&
            r->io.conn.read_so_far + buffered + toread > r->io.conn.read_limit)
        {
            toread = r->io.conn.read_limit - r->io.conn.read_so_far - buffered;
        }
        int retval = connRead(r->io.conn.conn,
                          (char*)r->io.conn.buf + sdslen(r->io.conn.buf),
                          toread);
        if (retval == 0) {
            // 对端关闭连接
            return 0;
        } else if (retval < 0) {
            // 如果是可重试错误则继续循环读取
            if (connLastErrorRetryable(r->io.conn.conn)) continue;
            if (errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0;
        }
        sdsIncrLen(r->io.conn.buf, retval);
    }

    // 将请求的数据从缓冲区拷贝到调用者提供的 buf 中
    memcpy(buf, (char*)r->io.conn.buf + r->io.conn.pos, len);
    r->io.conn.read_so_far += len;
    r->io.conn.pos += len;
    return len;
}

/* 返回连接上已读取的总字节数（用作 read/write 位置）。 */
static off_t rioConnTell(rio *r) {
    return r->io.conn.read_so_far;
}

/* 刷新缓冲区到目标设备（如适用）。
 * 返回值：成功返回 1，失败返回 0。 */
static int rioConnFlush(rio *r) {
    /* 此处的 flush 由 rioConnWrite 实现：
     * 当 buf 为 NULL 且 len 为 0 时，write 将其视作 flush 请求。 */
    return rioConnWrite(r,NULL,0);
}

/* rioConnIO：基于 connection 的 rio 实现模板。 */
static const rio rioConnIO = {
    rioConnRead,
    rioConnWrite,
    rioConnTell,
    rioConnFlush,
    NULL,           /* update_checksum：默认无校验和 */
    0,              /* 当前校验和 */
    0,              /* flags：标志位 */
    0,              /* 已读取或已写入的字节数 */
    0,              /* 单次读写块大小 */
    { { NULL, 0 } } /* io-specific 变量联合体 */
};

/* 创建一个从 connection 进行带缓冲读取的 rio 流。
 * 当读取字节数达到 read_limit 时停止缓冲；传入 0 表示无限制。 */
void rioInitWithConn(rio *r, connection *conn, size_t read_limit) {
    *r = rioConnIO;
    r->io.conn.conn = conn;
    r->io.conn.pos = 0;
    r->io.conn.read_limit = read_limit;
    r->io.conn.read_so_far = 0;
    r->io.conn.buf = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(r->io.conn.buf);
}

/* 释放 connection 类型的 rio 流。
 * 如果传入了 sds 指针 'remaining' 且缓冲区中还有未读数据，
 * 则将剩余未读数据通过 'remaining' 返回给调用者。 */
void rioFreeConn(rio *r, sds *remaining) {
    if (remaining && (size_t)r->io.conn.pos < sdslen(r->io.conn.buf)) {
        if (r->io.conn.pos > 0) sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        *remaining = r->io.conn.buf;
    } else {
        sdsfree(r->io.conn.buf);
        if (remaining) *remaining = NULL;
    }
    r->io.conn.buf = NULL;
}

/* ------------------- 文件描述符实现 ------------------
 * 该后端用于在无盘复制（diskless replication）场景下，
 * 由主节点直接将 RDB 数据流式写入管道（pipe）到从节点，
 * 而不在磁盘上生成 RDB 镜像。
 * 该后端只实现了写入，不支持读取。 */

/* fd I/O 后端：将数据写入到文件描述符指向的设备。
 * 返回值：成功返回 1，失败返回 0。
 *
 * 当 buf 为 NULL 且 len 为 0 时，该函数会执行 flush 操作
 * （即把已缓冲的数据写入底层 fd），因此它也用于实现 rioFdFlush()。 */
static size_t rioFdWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    unsigned char *p = (unsigned char*) buf;
    int doflush = (buf == NULL && len == 0);

    /* 对于较小的写入，先将数据缓存在用户态缓冲区中，
     * 仅当缓冲增长到一定大小时再刷到底层；
     * 而对于较大的写入，则倾向于先刷新已有缓冲区，
     * 然后直接将新数据写入，避免额外的内存分配与拷贝。 */
    if (len > PROTO_IOBUF_LEN) {
        /* 首先刷新任何已缓冲的数据。 */
        if (sdslen(r->io.fd.buf)) {
            if (rioFdWrite(r, NULL, 0) == 0)
                return 0;
        }
        /* 直接写入新数据，'p' 和 'len' 保留为入参。 */
    } else {
        if (len) {
            // 累积到用户态缓冲区
            r->io.fd.buf = sdscatlen(r->io.fd.buf,buf,len);
            // 缓冲区超过阈值则标记为需要刷盘
            if (sdslen(r->io.fd.buf) > PROTO_IOBUF_LEN)
                doflush = 1;
            if (!doflush)
                return 1;
        }
        /* 准备刷盘：相应地调整 'p' 和 'len'，使其指向缓冲区内容。 */
        p = (unsigned char*) r->io.fd.buf;
        len = sdslen(r->io.fd.buf);
    }

    size_t nwritten = 0;
    while(nwritten != len) {
        retval = write(r->io.fd.fd,p+nwritten,len-nwritten);
        if (retval <= 0) {
            // 被信号中断则继续写
            if (retval == -1 && errno == EINTR) continue;
            /* 该 rio 后端的唯一使用者使用阻塞 I/O。
             * 在阻塞 I/O 下，只有设置了 SO_SNDTIMEO 套接字选项时
             * 才可能返回 EWOULDBLOCK，因此这里将其翻译成
             * 更易于理解的错误码。 */
            if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0; /* 出错。 */
        }
        nwritten += retval;
    }

    r->io.fd.pos += len;
    sdsclear(r->io.fd.buf);
    return 1;
}

/* fd I/O 后端：从文件描述符读取数据。
 * 返回值：成功返回 1，失败返回 0。 */
static size_t rioFdRead(rio *r, void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* 错误：该 rio 后端不支持读取。 */
}

/* 返回 fd 上已写入的总字节数（用作 read/write 位置）。 */
static off_t rioFdTell(rio *r) {
    return r->io.fd.pos;
}

/* 刷新缓冲区到目标设备（如适用）。
 * 返回值：成功返回 1，失败返回 0。 */
static int rioFdFlush(rio *r) {
    /* 此处的 flush 由 rioFdWrite 实现：
     * 当 buf 为 NULL 且 len 为 0 时，write 将其视作 flush 请求。 */
    return rioFdWrite(r,NULL,0);
}

/* rioFdIO：基于文件描述符的 rio 实现模板。 */
static const rio rioFdIO = {
    rioFdRead,
    rioFdWrite,
    rioFdTell,
    rioFdFlush,
    NULL,           /* update_checksum：默认无校验和 */
    0,              /* 当前校验和 */
    0,              /* flags：标志位 */
    0,              /* 已读取或已写入的字节数 */
    0,              /* 单次读写块大小 */
    { { NULL, 0 } } /* io-specific 变量联合体 */
};

/* 使用给定的文件描述符 fd 初始化一个基于 fd 的 rio 流。
 * 该 rio 流会以用户态缓冲方式写入 fd。 */
void rioInitWithFd(rio *r, int fd) {
    *r = rioFdIO;
    r->io.fd.fd = fd;
    r->io.fd.pos = 0;
    r->io.fd.buf = sdsempty();
}

/* 释放 fd 类型的 rio 流（释放内部缓冲区）。 */
void rioFreeFd(rio *r) {
    sdsfree(r->io.fd.buf);
}

/* ---------------------------- 通用函数 ---------------------------- */

/* 通用的校验和更新函数：可被内存型和文件型 rio 安装，
 * 用于在需要校验和计算时更新 rio 内部维护的 cksum。 */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* 为基于文件的 rio 对象设置自动 fsync：每写入 'bytes' 字节自动 fsync 一次。
 * 默认值为 0，表示不进行任何自动 fsync。
 *
 * 该特性在某些场景下非常有用：当依赖操作系统自身的写缓冲时，
 * 操作系统有时会一次性缓冲过多数据，导致磁盘 I/O 在短时间内
 * 集中爆发；而通过显式 fsync 则可以将 I/O 压力在时间上分散开。 */
void rioSetAutoSync(rio *r, off_t bytes) {
    // 仅对基于文件的 rio 生效
    if(r->write != rioFileIO.write) return;
    r->io.file.autosync = bytes;
}

/* 为基于文件的 rio 对象设置是否在每次自动 sync 后回收页缓存。
 * 在 Linux 上，POSIX_FADV_DONTNEED 会跳过脏页，
 * 因此如果未启用 auto sync，则此选项不会生效。
 *
 * 该特性可以减小文件所占据的页缓存大小。 */
void rioSetReclaimCache(rio *r, int enabled) {
    r->io.file.reclaim_cache = enabled;
}

/* 检查 rio 的具体后端类型。
 * 通过比较 read 函数指针来识别是哪一种 rio 实现。 */
uint8_t rioCheckType(rio *r) {
    if (r->read == rioFileRead) {
        return RIO_TYPE_FILE;
    } else if (r->read == rioBufferRead) {
        return RIO_TYPE_BUFFER;
    } else if (r->read == rioConnRead) {
        return RIO_TYPE_CONN;
    } else {
        /* r->read == rioFdRead */
        return RIO_TYPE_FD;
    }
}

/* --------------------------- 高级接口 --------------------------
 *
 * 以下高级函数基于底层 rio.c 函数实现，用于辅助
 * 生成 AOF（Append Only File）所使用的 RESP 协议内容。 */

/* 写入 multi bulk count，格式为： "*<count>\r\n"。
 * 参数 prefix 通常为 '*'。返回实际写入的字节数，失败返回 0。 */
size_t rioWriteBulkCount(rio *r, char prefix, long count) {
    char cbuf[128];
    int clen;

    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* 写入二进制安全字符串，格式为： "$<count>\r\n<payload>\r\n"。
 * 返回实际写入的字节数，失败返回 0。 */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* 写入一个 long long 类型的值，格式为： "$<count>\r\n<payload>\r\n"。
 * 返回实际写入的字节数，失败返回 0。 */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* 写入一个 double 类型的值，格式为： "$<count>\r\n<payload>\r\n"。
 * 内部使用 fpconv_dtoa 进行 double 到字符串的转换。
 * 返回实际写入的字节数，失败返回 0。 */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;
    dlen = fpconv_dtoa(d, dbuf);
    dbuf[dlen] = '\0';
    return rioWriteBulkString(r,dbuf,dlen);
}
