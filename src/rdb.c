/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"
#include "fpconv_dtoa.h"
#include "stream.h"
#include "functions.h"
#include "intset.h"  /* Compact integer set structure */
#include "bio.h"

#include <math.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h>

/* 当内部 RDB 结构损坏时调用此宏 */
#define rdbReportCorruptRDB(...) rdbReportError(1, __LINE__,__VA_ARGS__)
/* 当 RDB 读取失败时（可能是短读）调用此宏 */
#define rdbReportReadError(...) rdbReportError(0, __LINE__,__VA_ARGS__)

/* 该宏用于判断当前是否处于 RESTORE 命令的上下文中，而非加载 RDB 或 AOF */
#define isRestoreContext() \
    ((server.current_client == NULL || server.current_client->id == CLIENT_ID_AOF) ? 0 : 1)

char* rdbFileBeingLoaded = NULL; /* used for rdb checking on read error */
extern int rdbCheckMode;
void rdbCheckError(const char *fmt, ...);
void rdbCheckSetError(const char *fmt, ...);

#ifdef __GNUC__
void rdbReportError(int corruption_error, int linenum, char *reason, ...) __attribute__ ((format (printf, 3, 4)));
#endif
void rdbReportError(int corruption_error, int linenum, char *reason, ...) {
    va_list ap;
    char msg[1024];
    int len;

    len = snprintf(msg,sizeof(msg),
        "Internal error in RDB reading offset %llu, function at rdb.c:%d -> ",
        (unsigned long long)server.loading_loaded_bytes, linenum);
    va_start(ap,reason);
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
    va_end(ap);

    if (isRestoreContext()) {
        /* 如果处于 RESTORE 命令上下文中，仅传递错误。
         * 以 VERBOSE 级别记录日志并返回（不退出）。 */
        serverLog(LL_VERBOSE, "%s", msg);
        return;
    } else if (rdbCheckMode) {
        /* 如果处于 rdb 检查模式中，让检查器自行处理错误 */
        rdbCheckError("%s",msg);
    } else if (rdbFileBeingLoaded) {
        /* 如果正在从磁盘加载 rdb 文件，则运行 rdb 检查（并退出） */
        serverLog(LL_WARNING, "%s", msg);
        char *argv[2] = {"",rdbFileBeingLoaded};
        if (anetIsFifo(argv[1])) {
            /* 无法检查 RDB FIFO，因为我们无法重新打开 FIFO 并检查已经流式传输的数据 */
            rdbCheckError("Cannot check RDB that is a FIFO: %s", argv[1]);
            return;
        }
        redis_check_rdb_main(2,argv,NULL);
    } else if (corruption_error) {
        /* 在无盘加载中，遇到文件损坏时，记录日志并退出 */
        serverLog(LL_WARNING, "%s. Failure loading rdb format", msg);
    } else {
        /* 在无盘加载中，遇到短读（非文件损坏），
         * 记录日志后继续运行（不退出） */
        serverLog(LL_WARNING, "%s. Failure loading rdb format from socket, assuming connection error, resuming operation.", msg);
        return;
    }
    serverLog(LL_WARNING, "Terminating server after rdb file reading failure.");
    exit(1);
}

/* 将原始字节写入 RDB 流。
 * 出错时返回 -1，成功时返回写入的字节数。 */
ssize_t rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

/* 向 RDB 流写入一个"类型"字节（单字节无符号整数）。
 * 成功返回写入字节数，失败返回 -1。 */
int rdbSaveType(rio *rdb, unsigned char type) {
    return rdbWriteRaw(rdb,&type,1);
}

/* 从 RDB 格式中加载一个"类型"，即一个单字节无符号整数。
 * 此函数不仅用于加载对象类型，还用于加载特殊的"类型"，
 * 如文件结束类型、EXPIRE 类型等。 */
int rdbLoadType(rio *rdb) {
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

/* 此函数仅用于加载使用 RDB_OPCODE_EXPIRETIME 操作码存储的旧数据库。
 * 新版本 Redis 使用 RDB_OPCODE_EXPIRETIME_MS 操作码存储。
 * 出错时返回 -1，但这本身可能是一个有效时间，因此
 * 调用者应在调用此函数后通过 rioGetReadError() 检查读取错误。 */
time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
    if (rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
}

/* 以小端格式保存毫秒级时间戳（8 字节 int64_t）。 */
ssize_t rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
    memrev64ifbe(&t64); /* Store in little endian. */
    return rdbWriteRaw(rdb,&t64,8);
}

/* 此函数从 RDB 文件中加载时间。它会读取 RDB 的版本，
 * 因为在 Redis 5（RDB 版本 9）之前，此函数未能正确地在
 * 大小端之间转换数据，因此带有过期键的 RDB 文件无法在
 * 大端和小端系统之间共享（因为过期时间会完全错误）。
 * 修复方法就是调用 memrev64ifbe()，但如果对所有 RDB 版本
 * 都进行此修复，会对大端系统引入不兼容性：
 * 升级到 Redis 5 后，大端系统将无法再加载它们自己的旧 RDB 文件。
 * 因此，我们只对新 RDB 版本应用此修复，而旧 RDB 版本
 * 仍按过去的方式加载，从而允许大端系统加载它们自己的旧 RDB 文件。
 *
 * 发生 I/O 错误时函数返回 LLONG_MAX，但如果这本身也是一个
 * 有效存储值，调用者应在调用此函数后使用 rioGetReadError() 检查错误。 */
long long rdbLoadMillisecondTime(rio *rdb, int rdbver) {
    int64_t t64;
    if (rioRead(rdb,&t64,8) == 0) return LLONG_MAX;
    if (rdbver >= 9) /* 参考本函数顶部的注释 */
        memrev64ifbe(&t64); /* 如果系统是大端，则转换为大端 */
    return (long long)t64;
}

/* 以编码格式保存一个长度。第一个字节的高 2 位用于保存编码类型。
 * 有关编码类型的更多信息，请参阅 RDB_* 定义。 */
int rdbSaveLen(rio *rdb, uint64_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
        /* 保存 6 位长度 */
        buf[0] = (len&0xFF)|(RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* 保存 14 位长度 */
        buf[0] = ((len>>8)&0xFF)|(RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* 保存 32 位长度 */
        buf[0] = RDB_32BITLEN;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        uint32_t len32 = htonl(len);
        if (rdbWriteRaw(rdb,&len32,4) == -1) return -1;
        nwritten = 1+4;
    } else {
        /* 保存 64 位长度 */
        buf[0] = RDB_64BITLEN;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonu64(len);
        if (rdbWriteRaw(rdb,&len,8) == -1) return -1;
        nwritten = 1+8;
    }
    return nwritten;
}


/* 加载一个编码后的长度。如果加载到的长度是用 rdbSaveLen() 存储的普通长度，
 * 则读取的长度设置到 '*lenptr'。如果加载到的长度描述的是后续的特殊编码，
 * 则将 '*isencoded' 置为 1，并将编码格式存储到 '*lenptr'。
 *
 * 有关特殊编码的更多信息，请参阅 rdb.h 中的 RDB_ENC_* 定义。
 *
 * 出错时函数返回 -1，成功时返回 0。 */
int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr) {
    unsigned char buf[2];
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(rdb,buf,1) == 0) return -1;
    type = (buf[0]&0xC0)>>6;
    if (type == RDB_ENCVAL) {
        /* 读取一个 6 位的编码类型 */
        if (isencoded) *isencoded = 1;
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_6BITLEN) {
        /* 读取 6 位长度 */
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_14BITLEN) {
        /* 读取 14 位长度 */
        if (rioRead(rdb,buf+1,1) == 0) return -1;
        *lenptr = ((buf[0]&0x3F)<<8)|buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        /* 读取 32 位长度 */
        uint32_t len;
        if (rioRead(rdb,&len,4) == 0) return -1;
        *lenptr = ntohl(len);
    } else if (buf[0] == RDB_64BITLEN) {
        /* 读取 64 位长度 */
        uint64_t len;
        if (rioRead(rdb,&len,8) == 0) return -1;
        *lenptr = ntohu64(len);
    } else {
        rdbReportCorruptRDB(
            "Unknown length encoding %d in rdbLoadLen()",type);
        return -1; /* 不会执行到这里 */
    }
    return 0;
}

/* 该函数类似 rdbLoadLenByRef()，但直接返回从 RDB 流读取的值，
 * 出错时返回 RDB_LENERR（因为这个值过大，无法应用于任何 Redis 数据结构）。 */
uint64_t rdbLoadLen(rio *rdb, int *isencoded) {
    uint64_t len;

    if (rdbLoadLenByRef(rdb,isencoded,&len) == -1) return RDB_LENERR;
    return len;
}

/* 当"value"参数可被编码类型的支持范围容纳时，将其编码为整数。
 * 若成功编码整数，则表示存储到由 "enc" 指向的缓冲区中并返回字符串长度。
 * 否则返回 0。 */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* 加载一个以整数编码的对象，指定的编码类型为 "enctype"。
 * 返回值根据 flags 而变化，详见 rdbGenericLoadStringObject()。 */
void *rdbLoadIntegerObject(rio *rdb, int enctype, int flags, size_t *lenptr) {
    int plainFlag = flags & RDB_LOAD_PLAIN;
    int sdsFlag = flags & RDB_LOAD_SDS;
    int hfldFlag = flags & (RDB_LOAD_HFLD|RDB_LOAD_HFLD_TTL);
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = ((uint32_t)enc[0])|
            ((uint32_t)enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = ((uint32_t)enc[0])|
            ((uint32_t)enc[1]<<8)|
            ((uint32_t)enc[2]<<16)|
            ((uint32_t)enc[3]<<24);
        val = (int32_t)v;
    } else {
        rdbReportCorruptRDB("Unknown RDB integer encoding type %d",enctype);
        return NULL; /* Never reached. */
    }
    if (plainFlag || sdsFlag || hfldFlag) {
        char buf[LONG_STR_SIZE], *p;
        int len = ll2string(buf,sizeof(buf),val);
        if (lenptr) *lenptr = len;
        if (plainFlag) {
            p = zmalloc(len);
        } else if (sdsFlag) {
            p = sdsnewlen(SDS_NOINIT,len);
        } else { /* hfldFlag */
            p = hfieldNew(NULL, len, (flags&RDB_LOAD_HFLD) ? 0 : 1);
        }
        memcpy(p,buf,len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLongForValue(val);
    } else {
        return createStringObjectFromLongLongWithSds(val);
    }
}

/* 形如 "2391" "-100" 这种不带任何空格的字符串对象，
 * 且取值范围可容纳在 8、16 或 32 位有符号整数内时，
 * 可以被编码为整数以节省空间。 */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    if (string2ll(s, len, &value)) {
        return rdbEncodeInteger(value, enc);
    } else {
        return 0;
    }
}

/* 将已压缩的 LZF 数据块写入 RDB 流。
 * 写入格式：[ENC_LZF 类型][压缩长度][原始长度][压缩数据]
 * 出错时返回 -1，成功时返回写入的字节数。 */
ssize_t rdbSaveLzfBlob(rio *rdb, void *data, size_t compress_len,
                       size_t original_len) {
    unsigned char byte;
    ssize_t n, nwritten = 0;

    /* 数据已被压缩！将其保存到磁盘上 */
    byte = (RDB_ENCVAL<<6)|RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,compress_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,original_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb,data,compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;

writeerr:
    return -1;
}

/* 尝试对字符串进行 LZF 压缩后保存到 RDB。
 * 若字符串长度 <= 4 或压缩无效则返回 0，
 * 成功时返回写入字节数，出错返回 -1。 */
ssize_t rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    void *out;

    /* 我们要求至少能压缩 4 字节，否则压缩不值得 */
    if (len <= 4) return 0;
    outlen = len-4;
    if ((out = zmalloc(outlen+1)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    ssize_t nwritten = rdbSaveLzfBlob(rdb, out, comprlen, len);
    zfree(out);
    return nwritten;
}

/* 从 RDB 格式中加载一个 LZF 压缩字符串。
 * 返回值根据 'flags' 而变化，更多信息请参阅
 * rdbGenericLoadStringObject() 函数。 */
void *rdbLoadLzfStringObject(rio *rdb, int flags, size_t *lenptr) {
    int plainFlag = flags & RDB_LOAD_PLAIN;
    int sdsFlag = flags & RDB_LOAD_SDS;
    int hfldFlag = flags & (RDB_LOAD_HFLD | RDB_LOAD_HFLD_TTL);
    int robjFlag = (!(plainFlag || sdsFlag || hfldFlag)); /* not plain/sds/hfld */

    uint64_t len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((c = ztrymalloc(clen)) == NULL) {
        serverLog(isRestoreContext()? LL_VERBOSE: LL_WARNING, "rdbLoadLzfStringObject failed allocating %llu bytes", (unsigned long long)clen);
        goto err;
    }

    /* 根据解压缩后的大小分配目标缓冲区 */
    if (plainFlag) {
        val = ztrymalloc(len);
    } else if (sdsFlag || robjFlag) {
        val = sdstrynewlen(SDS_NOINIT,len);
    } else { /* hfldFlag */
        val = hfieldTryNew(NULL, len, (flags&RDB_LOAD_HFLD) ? 0 : 1);
    }

    if (!val) {
        serverLog(isRestoreContext()? LL_VERBOSE: LL_WARNING, "rdbLoadLzfStringObject failed allocating %llu bytes", (unsigned long long)len);
        goto err;
    }

    if (lenptr) *lenptr = len;

    /* 加载压缩表示并将其解压缩到目标缓冲区 */
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) != len) {
        rdbReportCorruptRDB("Invalid LZF compressed string");
        goto err;
    }
    zfree(c);

    return (robjFlag) ? createObject(OBJ_STRING,val) : (void *) val;

err:
    zfree(c);
    if (plainFlag) {
        zfree(val);
    } else if (sdsFlag || robjFlag) {
        sdsfree(val);
    } else { /* hfldFlag*/
        hfieldFree(val);
    }
    return NULL;
}

/* 将字符串对象以 [len][data] 形式保存到磁盘。
 * 如果该对象是整数值的字符串表示形式，则尝试以特殊形式保存 */
ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len) {
    int enclen;
    ssize_t n, nwritten = 0;

    /* 尝试整数编码 */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* 尝试 LZF 压缩 - 20 字节以下即使压缩 aaaaaaaaaaaaaaaaaa 也无法成功，
     * 因此跳过 */
    if (server.rdb_compression && len > 20) {
        n = rdbSaveLzfStringObject(rdb,s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* 返回值为 0 表示数据无法压缩，按原方式保存 */
    }

    /* 按原样存储 */
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* 将一个 long long 值保存为编码字符串或普通字符串 */
ssize_t rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    ssize_t n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0) {
        return rdbWriteRaw(rdb,buf,enclen);
    } else {
        /* 编码为字符串 */
        enclen = ll2string((char*)buf,32,value);
        serverAssert(enclen < 32);
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* 类似 rdbSaveRawString()，但传入的是一个 Redis 对象 */
ssize_t rdbSaveStringObject(rio *rdb, robj *obj) {
    /* 如果对象已经是整数编码，则避免先解码再重新编码 */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr);
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
}

/* 根据 flags 从 RDB 文件中加载字符串对象：
 *
 * RDB_LOAD_NONE (无标志)：加载一个未编码的 RDB 对象。
 * RDB_LOAD_ENC: 如果返回类型是 Redis 对象，则尝试以特殊方式
 *               编码以提高内存效率。当传入此标志时，函数不再
 *               保证 obj->ptr 是 SDS 字符串。
 * RDB_LOAD_PLAIN: 返回用 zmalloc() 分配的普通字符串，
 *                 而不是包含 sds 的 Redis 对象。
 * RDB_LOAD_SDS: 返回 SDS 字符串，而不是 Redis 对象。
 * RDB_LOAD_HFLD: 返回 hash 字段对象 (mstr)
 * RDB_LOAD_HFLD_TTL: 返回保留 TTL 元数据的 hash 字段
 *
 * 发生 I/O 错误时返回 NULL。
 */
void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr) {
    void *buf;
    int plainFlag = flags & RDB_LOAD_PLAIN;
    int sdsFlag = flags & RDB_LOAD_SDS;
    int hfldFlag = flags & (RDB_LOAD_HFLD|RDB_LOAD_HFLD_TTL);
    int robjFlag = (!(plainFlag || sdsFlag || hfldFlag)); /* not plain/sds/hfld */

    int isencoded;
    unsigned long long len;

    len = rdbLoadLen(rdb,&isencoded);
    if (len == RDB_LENERR) return NULL;

    if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,flags,lenptr);
        case RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb,flags,lenptr);
        default:
            rdbReportCorruptRDB("Unknown RDB string encoding type %llu",len);
            return NULL;
        }
    }

    /* 返回 robj */
    if (robjFlag) {
        robj *o = tryCreateStringObject(SDS_NOINIT,len);
        if (!o) {
            serverLog(isRestoreContext()? LL_VERBOSE: LL_WARNING, "rdbGenericLoadStringObject failed allocating %llu bytes", len);
            return NULL;
        }
        if (len && rioRead(rdb,o->ptr,len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    }

    /* plain/sds/hfld */
    if (plainFlag) {
        buf = ztrymalloc(len);
    } else if (sdsFlag) {
        buf = sdstrynewlen(SDS_NOINIT,len);
    }  else { /* hfldFlag */
        buf = hfieldTryNew(NULL, len, (flags&RDB_LOAD_HFLD) ? 0 : 1);
    }
    if (!buf) {
        serverLog(isRestoreContext()? LL_VERBOSE: LL_WARNING, "rdbGenericLoadStringObject failed allocating %llu bytes", len);
        return NULL;
    }

    if (lenptr) *lenptr = len;
    if (len && rioRead(rdb,buf,len) == 0) {
        if (plainFlag)
            zfree(buf);
        else if (sdsFlag) {
            sdsfree(buf);
        } else { /* hfldFlag */
            hfieldFree(buf);
        }
        return NULL;
    }
    return buf;
}

/* 从 RDB 加载字符串对象（未编码版本）。 */
robj *rdbLoadStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
}

/* 从 RDB 加载字符串对象（尝试使用特殊编码以节省内存）。 */
robj *rdbLoadEncodedStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_ENC,NULL);
}

/* 保存一个 double 值。double 值以前缀为一个 8 位无符号整数
 * （指定表示长度）的字符串形式保存。
 * 该 8 位整数具有特殊值以表示以下情况：
 * 253：非数字 (NaN)
 * 254：+inf
 * 255：-inf
 */
ssize_t rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
        long long lvalue;
        /* 整数打印函数快得多，检查是否可以安全使用 */
        if (double2ll(val, &lvalue))
            ll2string((char*)buf+1,sizeof(buf)-1,lvalue);
        else {
            const int dlen = fpconv_dtoa(val, (char*)buf+1);
            buf[dlen+1] = '\0';
        }
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb,buf,len);
}

/* 关于 double 序列化的详细信息，请参阅 rdbSaveDoubleValue() */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(rdb,&len,1) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        if (sscanf(buf, "%lg", val)!=1) return -1;
        return 0;
    }
}

/* 为 RDB 8 或更高版本保存 double 值，假定采用 IE754 binary64 格式。
 * 我们只需确保始终以小端存储，否则直接将值从内存原样拷贝到磁盘。
 *
 * 出错时返回 -1，成功时返回序列化值的大小。 */
int rdbSaveBinaryDoubleValue(rio *rdb, double val) {
    memrev64ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* 从 RDB 8 或更高版本加载 double。更多信息请参阅 rdbSaveBinaryDoubleValue()。
 * 出错时返回 -1，否则返回 0。 */
int rdbLoadBinaryDoubleValue(rio *rdb, double *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
}

/* 类似 rdbSaveBinaryDoubleValue()，但使用单精度 */
int rdbSaveBinaryFloatValue(rio *rdb, float val) {
    memrev32ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* 类似 rdbLoadBinaryDoubleValue()，但使用单精度 */
int rdbLoadBinaryFloatValue(rio *rdb, float *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev32ifbe(val);
    return 0;
}

/* 保存对象 "o" 的对象类型 */
int rdbSaveObjectType(rio *rdb, robj *o) {
    switch (o->type) {
    case OBJ_STRING:
        return rdbSaveType(rdb,RDB_TYPE_STRING);
    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST || o->encoding == OBJ_ENCODING_LISTPACK)
            return rdbSaveType(rdb, RDB_TYPE_LIST_QUICKLIST_2);
        else
            serverPanic("Unknown list encoding");
    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET)
            return rdbSaveType(rdb,RDB_TYPE_SET_INTSET);
        else if (o->encoding == OBJ_ENCODING_HT)
            return rdbSaveType(rdb,RDB_TYPE_SET);
        else if (o->encoding == OBJ_ENCODING_LISTPACK)
            return rdbSaveType(rdb,RDB_TYPE_SET_LISTPACK);
        else
            serverPanic("Unknown set encoding");
    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_LISTPACK)
            return rdbSaveType(rdb,RDB_TYPE_ZSET_LISTPACK);
        else if (o->encoding == OBJ_ENCODING_SKIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_ZSET_2);
        else
            serverPanic("Unknown sorted set encoding");
    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_LISTPACK)
            return rdbSaveType(rdb,RDB_TYPE_HASH_LISTPACK);
        else if (o->encoding == OBJ_ENCODING_LISTPACK_EX)
            return rdbSaveType(rdb,RDB_TYPE_HASH_LISTPACK_EX);
        else if (o->encoding == OBJ_ENCODING_HT) {
            if (hashTypeGetMinExpire(o, /*accurate*/ 1) == EB_EXPIRE_TIME_INVALID)
                return rdbSaveType(rdb,RDB_TYPE_HASH);
            else
                return rdbSaveType(rdb,RDB_TYPE_HASH_METADATA);
        } else
            serverPanic("Unknown hash encoding");
    case OBJ_STREAM:
        return rdbSaveType(rdb,RDB_TYPE_STREAM_LISTPACKS_3);
    case OBJ_MODULE:
        return rdbSaveType(rdb,RDB_TYPE_MODULE_2);
    default:
        serverPanic("Unknown object type");
    }
    return -1; /* 避免编译器警告 */
}

/* 使用 rdbLoadType() 从 RDB 格式加载一个 TYPE，但如果该类型
 * 不是有效的对象类型，则返回 -1 */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

/* 此辅助函数将消费者组的待处理条目列表 (PEL) 序列化到 RDB 文件。
 * 'nacks' 参数告诉函数是否同时持久化未确认消息的信息，
 * 还是仅持久化 ID：这是有用的，因为对于全局消费者组 PEL，
 * 我们同时也序列化 NACK；而在序列化本地消费者 PEL 时仅添加 ID，
 * 这些 ID 将在全局 PEL 内部被解析以引用同一结构。 */
ssize_t rdbSaveStreamPEL(rio *rdb, rax *pel, int nacks) {
    ssize_t n, nwritten = 0;

    /* PEL 中的条目数 */
    if ((n = rdbSaveLen(rdb,raxSize(pel))) == -1) return -1;
    nwritten += n;

    /* 保存每个条目 */
    raxIterator ri;
    raxStart(&ri,pel);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        /* 我们以原始 128 位大端数形式存储 ID，
         * 就像它们在 radix tree 键中那样。 */
        if ((n = rdbWriteRaw(rdb,ri.key,sizeof(streamID))) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        if (nacks) {
            streamNACK *nack = ri.data;
            if ((n = rdbSaveMillisecondTime(rdb,nack->delivery_time)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            if ((n = rdbSaveLen(rdb,nack->delivery_count)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            /* 我们不保存消费者名称：我们将在消费者 PEL 中保存
             * 每个消费者的待处理 ID，并在加载时解析消费者。 */
        }
    }
    raxStop(&ri);
    return nwritten;
}

/* 将 stream 消费者组中的消费者序列化到 RDB。
 * stream 数据类型序列化的辅助函数。此处我们对每个消费者
 * 持久化其元数据及其 PEL。 */
size_t rdbSaveStreamConsumers(rio *rdb, streamCG *cg) {
    ssize_t n, nwritten = 0;

    /* 此消费者组中的消费者数量 */
    if ((n = rdbSaveLen(rdb,raxSize(cg->consumers))) == -1) return -1;
    nwritten += n;

    /* 保存每个消费者 */
    raxIterator ri;
    raxStart(&ri,cg->consumers);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        streamConsumer *consumer = ri.data;

        /* 消费者名称 */
        if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        /* seen_time */
        if ((n = rdbSaveMillisecondTime(rdb,consumer->seen_time)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        /* active_time */
        if ((n = rdbSaveMillisecondTime(rdb,consumer->active_time)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        /* 消费者 PEL，不包含 ACK（请参见函数最后一个参数取值为 0），
         * 在加载时我们将在消费者组的全局 PEL 中查找 ID，
         * 并在消费者本地 PEL 中放置一个引用。 */
        if ((n = rdbSaveStreamPEL(rdb,consumer->pel,0)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;
    }
    raxStop(&ri);
    return nwritten;
}

/* 保存一个 Redis 对象。
 * 出错时返回 -1，成功时返回写入的字节数。 */
ssize_t rdbSaveObject(rio *rdb, robj *o, robj *key, int dbid) {
    ssize_t n = 0, nwritten = 0;

    if (o->type == OBJ_STRING) {
        /* 保存字符串值 */
        if ((n = rdbSaveStringObject(rdb,o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == OBJ_LIST) {
        /* 保存列表值 */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;

            if ((n = rdbSaveLen(rdb,ql->len)) == -1) return -1;
            nwritten += n;

            while(node) {
                if ((n = rdbSaveLen(rdb,node->container)) == -1) return -1;
                nwritten += n;

                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    size_t compress_len = quicklistGetLzf(node, &data);
                    if ((n = rdbSaveLzfBlob(rdb,data,compress_len,node->sz)) == -1) return -1;
                    nwritten += n;
                } else {
                    if ((n = rdbSaveRawString(rdb,node->entry,node->sz)) == -1) return -1;
                    nwritten += n;
                }
                node = node->next;
            }
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            unsigned char *lp = o->ptr;

            /* 将 list listpack 存为只有一个节点的伪 quicklist */
            if ((n = rdbSaveLen(rdb,1)) == -1) return -1;
            nwritten += n;
            if ((n = rdbSaveLen(rdb,QUICKLIST_NODE_CONTAINER_PACKED)) == -1) return -1;
            nwritten += n;
            if ((n = rdbSaveRawString(rdb,lp,lpBytes(lp))) == -1) return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        /* 保存集合值 */
        if (o->encoding == OBJ_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                sds ele = dictGetKey(de);
                if ((n = rdbSaveRawString(rdb,(unsigned char*)ele,sdslen(ele)))
                    == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            size_t l = lpBytes((unsigned char *)o->ptr);
            if ((n = rdbSaveRawString(rdb, o->ptr, l)) == -1) return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* 保存有序集合值 */
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            size_t l = lpBytes((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            zskiplist *zsl = zs->zsl;

            if ((n = rdbSaveLen(rdb,zsl->length)) == -1) return -1;
            nwritten += n;

            /* 我们按从大到小的顺序保存 skiplist 元素
             * （因为元素在 skiplist 中已经有序，所以这是显而易见的）：
             * 这可以改进加载过程，因为接下来要加载的元素总是较小的，
             * 因此向 skiplist 插入时总能立即停在表头，
             * 使插入复杂度为 O(1) 而不是 O(log(N))。 */
            zskiplistNode *zn = zsl->tail;
            while (zn != NULL) {
                if ((n = rdbSaveRawString(rdb,
                    (unsigned char*)zn->ele,sdslen(zn->ele))) == -1)
                {
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveBinaryDoubleValue(rdb,zn->score)) == -1)
                    return -1;
                nwritten += n;
                zn = zn->backward;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        /* 保存哈希值 */
        if ((o->encoding == OBJ_ENCODING_LISTPACK) ||
            (o->encoding == OBJ_ENCODING_LISTPACK_EX))
        {
            /* 如果需要，保存 min/next HFE 过期时间 */
            if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
                uint64_t minExpire = hashTypeGetMinExpire(o, 0);
                /* 如果时间无效则保存 0 */
                if (minExpire == EB_EXPIRE_TIME_INVALID)
                    minExpire = 0;
                if (rdbSaveMillisecondTime(rdb, minExpire) == -1)
                    return -1;
            }
            unsigned char *lp_ptr = hashTypeListpackGetLp(o);
            size_t l = lpBytes(lp_ptr);

            if ((n = rdbSaveRawString(rdb,lp_ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_HT) {
            int hashWithMeta = 0;  /* RDB_TYPE_HASH_METADATA */
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;
            /* 根据是否存在至少一个带有效 TTL 的字段，
             * 决定要使用的哈希布局。如果存在这样的字段，
             * 则采用 RDB_TYPE_HASH_METADATA 布局，包含 [ttl][field][value] 元组。
             * 否则使用标准的 RDB_TYPE_HASH 布局，仅包含 [field][value] 元组。 */
            uint64_t minExpire = hashTypeGetMinExpire(o, 1);

            /* 如果是 RDB_TYPE_HASH_METADATA（字段上可以有 TTL） */
            if (minExpire != EB_EXPIRE_TIME_INVALID) {
                hashWithMeta = 1;
                /* 保存哈希的下一个字段过期时间 */
                if (rdbSaveMillisecondTime(rdb, minExpire) == -1) {
                    dictReleaseIterator(di);
                    return -1;
                }
            }

            /* 保存哈希中的字段数 */
            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;

            /* 保存所有哈希字段 */
            while((de = dictNext(di)) != NULL) {
                hfield field = dictGetKey(de);
                sds value = dictGetVal(de);

                /* 保存 TTL */
                if (hashWithMeta) {
                    uint64_t ttl, expiryTime= hfieldGetExpireTime(field);

                    /* 已保存的 TTL 值：
                     *  - 0：表示无 TTL。这是常见情况，所以保持较小值。
                     *  - 其他值：TTL 相对于 minExpire（+1 以避免与已使用的 0 冲突）
                     */
                    ttl = (expiryTime == EB_EXPIRE_TIME_INVALID) ? 0 : expiryTime - minExpire + 1;
                    if ((n = rdbSaveLen(rdb, ttl)) == -1) {
                        dictReleaseIterator(di);
                        return -1;
                    }
                    nwritten += n;
                }

                /* 保存键 */
                if ((n = rdbSaveRawString(rdb,(unsigned char*)field,
                        hfieldlen(field))) == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;

                /* 保存值 */
                if ((n = rdbSaveRawString(rdb,(unsigned char*)value,
                        sdslen(value))) == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_STREAM) {
        /* 存储 radix tree 中有多少个 listpack */
        stream *s = o->ptr;
        rax *rax = s->rax;
        if ((n = rdbSaveLen(rdb,raxSize(rax))) == -1) return -1;
        nwritten += n;

        /* 按原样序列化 radix tree 中的所有 listpack，
         * 加载时我们将使用每个 listpack 的第一个条目
         * 重新插入到 radix tree 中。 */
        raxIterator ri;
        raxStart(&ri,rax);
        raxSeek(&ri,"^",NULL,0);
        while (raxNext(&ri)) {
            unsigned char *lp = ri.data;
            size_t lp_bytes = lpBytes(lp);
            if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            if ((n = rdbSaveRawString(rdb,lp,lp_bytes)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
        }
        raxStop(&ri);

        /* 保存 stream 中的元素数量。我们之后无法轻松获取该值，
         * 因为需要检查宏节点中条目数量：这不是一个很好的 CPU/空间折衷方案。 */
        if ((n = rdbSaveLen(rdb,s->length)) == -1) return -1;
        nwritten += n;
        /* 保存最后一个条目 ID */
        if ((n = rdbSaveLen(rdb,s->last_id.ms)) == -1) return -1;
        nwritten += n;
        if ((n = rdbSaveLen(rdb,s->last_id.seq)) == -1) return -1;
        nwritten += n;
        /* 保存第一个条目 ID */
        if ((n = rdbSaveLen(rdb,s->first_id.ms)) == -1) return -1;
        nwritten += n;
        if ((n = rdbSaveLen(rdb,s->first_id.seq)) == -1) return -1;
        nwritten += n;
        /* 保存最大的墓碑 ID */
        if ((n = rdbSaveLen(rdb,s->max_deleted_entry_id.ms)) == -1) return -1;
        nwritten += n;
        if ((n = rdbSaveLen(rdb,s->max_deleted_entry_id.seq)) == -1) return -1;
        nwritten += n;
        /* 保存 offset */
        if ((n = rdbSaveLen(rdb,s->entries_added)) == -1) return -1;
        nwritten += n;

        /* 消费者组及其客户端是 stream 类型的一部分，
         * 因此需要序列化每个消费者组。 */

        /* 保存消费者组的数量 */
        size_t num_cgroups = s->cgroups ? raxSize(s->cgroups) : 0;
        if ((n = rdbSaveLen(rdb,num_cgroups)) == -1) return -1;
        nwritten += n;

        if (num_cgroups) {
            /* 序列化每个消费者组 */
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                streamCG *cg = ri.data;

                /* 保存消费者组名称 */
                if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* 最后 ID */
                if ((n = rdbSaveLen(rdb,cg->last_id.ms)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveLen(rdb,cg->last_id.seq)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* 保存消费者组的逻辑读取计数器 */
                if ((n = rdbSaveLen(rdb,cg->entries_read)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* 保存全局 PEL */
                if ((n = rdbSaveStreamPEL(rdb,cg->pel,1)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* 保存该消费者组的消费者 */
                if ((n = rdbSaveStreamConsumers(rdb,cg)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;
            }
            raxStop(&ri);
        }
    } else if (o->type == OBJ_MODULE) {
        /* 保存模块特定的值 */
        RedisModuleIO io;
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;

        /* 写入 "module" 标识符作为前缀，以便加载时能够调用正确的模块 */
        int retval = rdbSaveLen(rdb,mt->id);
        if (retval == -1) return -1;
        moduleInitIOContext(io,mt,rdb,key,dbid);
        io.bytes += retval;

        /* 然后写入模块特定的表示 + EOF 标记 */
        mt->rdb_save(&io,mv->value);
        retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_EOF);
        if (retval == -1)
            io.error = 1;
        else
            io.bytes += retval;

        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }
        return io.error ? -1 : (ssize_t)io.bytes;
    } else {
        serverPanic("Unknown object type");
    }
    return nwritten;
}

/* 返回对象使用 rdbSaveObject() 保存时在磁盘上占用的长度。
 * 目前我们使用一种小技巧来获取该长度，对代码的改动非常小。
 * 将来我们可以切换到更快的方案。 */
size_t rdbSavedObjectLen(robj *o, robj *key, int dbid) {
    ssize_t len = rdbSaveObject(NULL,o,key,dbid);
    serverAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* 保存一个键值对，包括过期时间、类型、键、值。
 * 出错时返回 -1。
 * 成功时，如果键确实被保存，则返回 1。 */
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, long long expiretime, int dbid) {
    int savelru = server.maxmemory_policy & MAXMEMORY_FLAG_LRU;
    int savelfu = server.maxmemory_policy & MAXMEMORY_FLAG_LFU;

    /* 保存过期时间 */
    if (expiretime != -1) {
        if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

    /* 保存 LRU 信息 */
    if (savelru) {
        uint64_t idletime = estimateObjectIdleTime(val);
        idletime /= 1000; /* 使用秒为单位就足够了，且占用的空间更少 */
        if (rdbSaveType(rdb,RDB_OPCODE_IDLE) == -1) return -1;
        if (rdbSaveLen(rdb,idletime) == -1) return -1;
    }

    /* 保存 LFU 信息 */
    if (savelfu) {
        uint8_t buf[1];
        buf[0] = LFUDecrAndReturn(val);
        /* 我们可以恰好用两个字节编码此信息：操作码和一个 8 位计数器，
         * 因为频率采用对数刻度，范围为 0-255。
         * 注意我们不存储减半时间，因为在加载时重置一次
         * 不会对频率产生太大影响。 */
        if (rdbSaveType(rdb,RDB_OPCODE_FREQ) == -1) return -1;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
    }

    /* 保存类型、键、值 */
    if (rdbSaveObjectType(rdb,val) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    if (rdbSaveObject(rdb,val,key,dbid) == -1) return -1;

    /* 如果需要（用于测试），在返回前增加延时 */
    if (server.rdb_key_save_delay)
        debugDelay(server.rdb_key_save_delay);

    return 1;
}

/* 保存一个 AUX 字段 */
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen) {
    ssize_t ret, len = 0;
    if ((ret = rdbSaveType(rdb,RDB_OPCODE_AUX)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,key,keylen)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,val,vallen)) == -1) return -1;
    len += ret;
    return len;
}

/* rdbSaveAuxField() 的封装，用于 key/val 长度可由 strlen() 获取的情况 */
ssize_t rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val) {
    return rdbSaveAuxField(rdb,key,strlen(key),val,strlen(val));
}

/* strlen(key) + 整数类型（最大到 long long 范围）的封装 */
ssize_t rdbSaveAuxFieldStrInt(rio *rdb, char *key, long long val) {
    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
}

/* 保存一些默认的 AUX 字段，包含有关生成的 RDB 的信息 */
int rdbSaveInfoAuxFields(rio *rdb, int rdbflags, rdbSaveInfo *rsi) {
    int redis_bits = (sizeof(void*) == 8) ? 64 : 32;
    int aof_base = (rdbflags & RDBFLAGS_AOF_PREAMBLE) != 0;

    /* 添加几个关于创建 RDB 时状态的字段 */
    if (rdbSaveAuxFieldStrStr(rdb,"redis-ver",REDIS_VERSION) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"redis-bits",redis_bits) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"ctime",time(NULL)) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) return -1;

    /* 处理会生成 aux 字段的保存选项 */
    if (rsi) {
        if (rdbSaveAuxFieldStrInt(rdb,"repl-stream-db",rsi->repl_stream_db)
            == -1) return -1;
        if (rdbSaveAuxFieldStrStr(rdb,"repl-id",server.replid)
            == -1) return -1;
        if (rdbSaveAuxFieldStrInt(rdb,"repl-offset",server.master_repl_offset)
            == -1) return -1;
    }
    if (rdbSaveAuxFieldStrInt(rdb, "aof-base", aof_base) == -1) return -1;
    return 1;
}

ssize_t rdbSaveSingleModuleAux(rio *rdb, int when, moduleType *mt) {
    /* 保存模块特定的 aux 值 */
    RedisModuleIO io;
    int retval = 0;
    moduleInitIOContext(io,mt,rdb,NULL,-1);

    /* 我们将 AUX 字段头部保存在一个临时缓冲区中，
     * 以便支持 aux_save2 API。如果使用了 aux_save2，
     * 该缓冲区会在模块第一次对 RDB 执行写操作时被刷新，
     * 且若模块未执行任何写操作则会被忽略。 */
    rio aux_save_headers_rio;
    rioInitWithBuffer(&aux_save_headers_rio, sdsempty());

    if (rdbSaveType(&aux_save_headers_rio, RDB_OPCODE_MODULE_AUX) == -1) goto error;

    /* 写入 "module" 标识符作为前缀，以便加载时能够调用正确的模块 */
    if (rdbSaveLen(&aux_save_headers_rio,mt->id) == -1) goto error;

    /* 写入 'when' 以便在加载时提供。为了向后兼容，添加一个 UINT 操作码，
     * MT 之后的所有内容都需要以操作码作为前缀。 */
    if (rdbSaveLen(&aux_save_headers_rio,RDB_MODULE_OPCODE_UINT) == -1) goto error;
    if (rdbSaveLen(&aux_save_headers_rio,when) == -1) goto error;

    /* 然后写入模块特定的表示 + EOF 标记 */
    if (mt->aux_save2) {
        io.pre_flush_buffer = aux_save_headers_rio.io.buffer.ptr;
        mt->aux_save2(&io,when);
        if (io.pre_flush_buffer) {
            /* aux_save 未将任何数据保存到 RDB。
             * 我们将不保存与此 aux 类型相关的任何数据，
             * 以便在模块不存在时也能加载该 RDB。 */
            sdsfree(io.pre_flush_buffer);
            io.pre_flush_buffer = NULL;
            return 0;
        }
    } else {
        /* 立即写入头部，aux_save 不会延迟保存头部 */
        retval = rdbWriteRaw(rdb, aux_save_headers_rio.io.buffer.ptr, sdslen(aux_save_headers_rio.io.buffer.ptr));
        if (retval == -1) goto error;
        io.bytes += retval;
        sdsfree(aux_save_headers_rio.io.buffer.ptr);
        mt->aux_save(&io,when);
    }
    retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_EOF);
    serverAssert(!io.pre_flush_buffer);
    if (retval == -1)
        io.error = 1;
    else
        io.bytes += retval;

    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    if (io.error)
        return -1;
    return io.bytes;
error:
    sdsfree(aux_save_headers_rio.io.buffer.ptr);
    return -1;
}

/* 将所有已注册的函数库序列化到 RDB 流。
 * 成功返回写入字节数，出错返回 -1。 */
ssize_t rdbSaveFunctions(rio *rdb) {
    dict *functions = functionsLibGet();
    dictIterator *iter = dictGetIterator(functions);
    dictEntry *entry = NULL;
    ssize_t written = 0;
    ssize_t ret;
    while ((entry = dictNext(iter))) {
        if ((ret = rdbSaveType(rdb, RDB_OPCODE_FUNCTION2)) < 0) goto werr;
        written += ret;
        functionLibInfo *li = dictGetVal(entry);
        if ((ret = rdbSaveRawString(rdb, (unsigned char *) li->code, sdslen(li->code))) < 0) goto werr;
        written += ret;
    }
    dictReleaseIterator(iter);
    return written;

werr:
    dictReleaseIterator(iter);
    return -1;
}

/* 将指定数据库的所有键值对序列化到 RDB 流。
 * 包括 SELECTDB、RESIZEDB 操作码以及每个条目。
 * 成功返回写入字节数，出错返回 -1。 */
ssize_t rdbSaveDb(rio *rdb, int dbid, int rdbflags, long *key_counter) {
    dictEntry *de;
    ssize_t written = 0;
    ssize_t res;
    kvstoreIterator *kvs_it = NULL;
    static long long info_updated_time = 0;
    char *pname = (rdbflags & RDBFLAGS_AOF_PREAMBLE) ? "AOF rewrite" :  "RDB";
    /* 根据保存类型选择进度信息的前缀名称 */

    redisDb *db = server.db + dbid;
    unsigned long long int db_size = kvstoreSize(db->keys);
    if (db_size == 0) return 0;

    /* 写入 SELECT DB 操作码 */
    if ((res = rdbSaveType(rdb,RDB_OPCODE_SELECTDB)) < 0) goto werr;
    written += res;
    if ((res = rdbSaveLen(rdb, dbid)) < 0) goto werr;
    written += res;

    /* 写入 RESIZE DB 操作码 */
    unsigned long long expires_size = kvstoreSize(db->expires);
    if ((res = rdbSaveType(rdb,RDB_OPCODE_RESIZEDB)) < 0) goto werr;
    written += res;
    if ((res = rdbSaveLen(rdb,db_size)) < 0) goto werr;
    written += res;
    if ((res = rdbSaveLen(rdb,expires_size)) < 0) goto werr;
    written += res;

    kvs_it = kvstoreIteratorInit(db->keys);
    int last_slot = -1;
    /* 遍历该数据库，写入每个条目 */
    while ((de = kvstoreIteratorNext(kvs_it)) != NULL) {
        int curr_slot = kvstoreIteratorGetCurrentDictIndex(kvs_it);
        /* 保存 slot 信息 */
        if (server.cluster_enabled && curr_slot != last_slot) {
            if ((res = rdbSaveType(rdb, RDB_OPCODE_SLOT_INFO)) < 0) goto werr;
            written += res;
            if ((res = rdbSaveLen(rdb, curr_slot)) < 0) goto werr;
            written += res;
            if ((res = rdbSaveLen(rdb, kvstoreDictSize(db->keys, curr_slot))) < 0) goto werr;
            written += res;
            if ((res = rdbSaveLen(rdb, kvstoreDictSize(db->expires, curr_slot))) < 0) goto werr;
            written += res;
            last_slot = curr_slot;
        }
        sds keystr = dictGetKey(de);
        robj key, *o = dictGetVal(de);
        long long expire;
        size_t rdb_bytes_before_key = rdb->processed_bytes;

        initStaticStringObject(key,keystr);
        expire = getExpire(db,&key);
        if ((res = rdbSaveKeyValuePair(rdb, &key, o, expire, dbid)) < 0) goto werr;
        written += res;

        /* 在 fork 出的子进程中，我们可以尝试将内存归还给 OS，
         * 并可能避免或减少 COW。我们向 dismiss 机制提供
         * 已存储对象估计大小的提示。 */
        size_t dump_size = rdb->processed_bytes - rdb_bytes_before_key;
        if (server.in_fork_child) dismissObject(o, dump_size);

        /* 大约每 1 秒更新一次子进程信息。
         * 为了避免在每次迭代中调用 mstime()，
         * 我们每处理 1024 个键检查一次时间差 */
        if (((*key_counter)++ & 1023) == 0) {
            long long now = mstime();
            if (now - info_updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, *key_counter, pname);
                info_updated_time = now;
            }
        }
    }
    kvstoreIteratorRelease(kvs_it);
    return written;

werr:
    if (kvs_it) kvstoreIteratorRelease(kvs_it);
    return -1;
}

/* 以 RDB 格式生成数据库转储，并将其发送到指定的 Redis I/O 通道。
 * 成功时返回 C_ERR，否则返回 C_OK；
 * 由于 I/O 错误，部分或全部输出可能会丢失。
 *
 * 当函数返回 C_ERR 且 'error' 不为 NULL 时，
 * 'error' 指向的整数将被设置为 I/O 错误发生后的 errno 值。 */
int rdbSaveRio(int req, rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {
    char magic[10];
    uint64_t cksum;
    long key_counter = 0;
    int j;

    if (server.rdb_checksum)
        rdb->update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
    if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;
    if (rdbSaveInfoAuxFields(rdb,rdbflags,rsi) == -1) goto werr;
    if (!(req & SLAVE_REQ_RDB_EXCLUDE_DATA) && rdbSaveModulesAux(rdb, REDISMODULE_AUX_BEFORE_RDB) == -1) goto werr;

    /* 保存 functions */
    if (!(req & SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS) && rdbSaveFunctions(rdb) == -1) goto werr;

    /* 保存所有数据库，如果处于仅函数模式则跳过此步骤 */
    if (!(req & SLAVE_REQ_RDB_EXCLUDE_DATA)) {
        for (j = 0; j < server.dbnum; j++) {
            if (rdbSaveDb(rdb, j, rdbflags, &key_counter) == -1) goto werr;
        }
    }

    if (!(req & SLAVE_REQ_RDB_EXCLUDE_DATA) && rdbSaveModulesAux(rdb, REDISMODULE_AUX_AFTER_RDB) == -1) goto werr;

    /* EOF 操作码 */
    if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 校验和。如果禁用了校验和计算，其值将为零，
     * 加载代码在这种情况下会跳过校验。 */
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(rdb,&cksum,8) == 0) goto werr;
    return C_OK;

werr:
    if (error) *error = errno;
    return C_ERR;
}

/* 此辅助函数仅用于无盘复制。
 * 它只是 rdbSaveRio() 的一个包装，在生成的 RDB 转储上
 * 额外添加前缀和后缀。前缀为：
 *
 * $EOF:<40 字节不可猜测的十六进制字符串>\r\n
 *
 * 后缀为我们在前缀中公布的 40 字节十六进制字符串。
 * 这样接收负载的进程无需处理内容即可知道负载的结束位置。 */
int rdbSaveRioWithEOFMark(int req, rio *rdb, int *error, rdbSaveInfo *rsi) {
    char eofmark[RDB_EOF_MARK_SIZE];

    startSaving(RDBFLAGS_REPLICATION);
    getRandomHexChars(eofmark,RDB_EOF_MARK_SIZE);
    if (error) *error = 0;
    if (rioWrite(rdb,"$EOF:",5) == 0) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    if (rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if (rdbSaveRio(req,rdb,error,RDBFLAGS_REPLICATION,rsi) == C_ERR) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    stopSaving(1);
    return C_OK;

werr: /* 写入错误 */
    /* 仅当 rdbSaveRio() 调用尚未设置 'error' 时才设置它 */
    if (error && *error == 0) *error = errno;
    stopSaving(0);
    return C_ERR;
}

/* RDB 保存的内部实现。将数据库转储写入指定文件。
 * 成功返回 C_OK，出错返回 C_ERR。 */
static int rdbSaveInternal(int req, const char *filename, rdbSaveInfo *rsi, int rdbflags) {
    char cwd[MAXPATHLEN]; /* 用于错误消息的当前工作目录路径 */
    rio rdb;
    int error = 0;
    int saved_errno;
    char *err_op;    /* 用于详细日志 */

    FILE *fp = fopen(filename,"w");
    if (!fp) {
        saved_errno = errno;
        char *str_err = strerror(errno);
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Failed opening the temp RDB file %s (in server root dir %s) "
            "for saving: %s",
            filename,
            cwdp ? cwdp : "unknown",
            str_err);
        errno = saved_errno;
        return C_ERR;
    }

    rioInitWithFile(&rdb,fp);

    if (server.rdb_save_incremental_fsync) {
        /* 启用增量 fsync 以避免一次性写入大量数据导致延迟 */
        rioSetAutoSync(&rdb,REDIS_AUTOSYNC_BYTES);
        if (!(rdbflags & RDBFLAGS_KEEP_CACHE)) rioSetReclaimCache(&rdb,1);
    }

    if (rdbSaveRio(req,&rdb,&error,rdbflags,rsi) == C_ERR) {
        errno = error;
        err_op = "rdbSaveRio";
        goto werr;
    }

    /* 确保数据不会残留在 OS 的输出缓冲区中 */
    if (fflush(fp)) { err_op = "fflush"; goto werr; }
    if (fsync(fileno(fp))) { err_op = "fsync"; goto werr; }
    if (!(rdbflags & RDBFLAGS_KEEP_CACHE) && reclaimFilePageCache(fileno(fp), 0, 0) == -1) {
        serverLog(LL_NOTICE,"Unable to reclaim cache after saving RDB: %s", strerror(errno));
    }
    if (fclose(fp)) { fp = NULL; err_op = "fclose"; goto werr; }

    return C_OK;

werr:
    saved_errno = errno;
    serverLog(LL_WARNING,"Write error while saving DB to the disk(%s): %s", err_op, strerror(errno));
    if (fp) fclose(fp);
    unlink(filename);
    errno = saved_errno;
    return C_ERR;
}

/* 将 DB 保存到文件。类似于 rdbSave()，但此函数不使用临时文件，
 * 也不会更新指标。 */
int rdbSaveToFile(const char *filename) {
    startSaving(RDBFLAGS_NONE);
    /* 通知模块保存开始 */

    if (rdbSaveInternal(SLAVE_REQ_NONE,filename,NULL,RDBFLAGS_NONE) != C_OK) {
        int saved_errno = errno;
        stopSaving(0);
        errno = saved_errno;
        return C_ERR;
    }

    stopSaving(1);
    return C_OK;
}

/* 将 DB 保存到磁盘。出错返回 C_ERR，成功返回 C_OK。 */
int rdbSave(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    char tmpfile[256];
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */

    startSaving(rdbflags);
    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());

    if (rdbSaveInternal(req,tmpfile,rsi,rdbflags) != C_OK) {
        stopSaving(0);
        return C_ERR;
    }
    
    /* 使用 RENAME 以确保仅当生成的 DB 文件正常时才原子地替换 DB 文件 */
    if (rename(tmpfile,filename) == -1) {
        char *str_err = strerror(errno);
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Error moving temp DB file %s on the final "
            "destination %s (in server root dir %s): %s",
            tmpfile,
            filename,
            cwdp ? cwdp : "unknown",
            str_err);
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    if (fsyncFileDir(filename) != 0) {
        serverLog(LL_WARNING,
            "Failed to fsync directory while saving DB: %s", strerror(errno));
        stopSaving(0);
        return C_ERR;
    }

    serverLog(LL_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = C_OK;
    stopSaving(1);
    return C_OK;
}

/* 在后台子进程中执行 RDB 保存（BGSAVE）。
 * 成功 fork 子进程返回 C_OK，出错返回 C_ERR。
 * 子进程会将 RDB 写入 filename 指定的文件。 */
int rdbSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    pid_t childpid;

    if (hasActiveChildProcess()) return C_ERR;
    server.stat_rdb_saves++;

    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = time(NULL);

    if ((childpid = redisFork(CHILD_TYPE_RDB)) == 0) {
        int retval;

        /* Child */
        redisSetProcTitle("redis-rdb-bgsave");
        redisSetCpuAffinity(server.bgsave_cpulist);
        retval = rdbSave(req, filename,rsi,rdbflags);
        if (retval == C_OK) {
            sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
        }
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            server.lastbgsave_status = C_ERR;
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,"Background saving started by pid %ld",(long) childpid);
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_type = RDB_CHILD_TYPE_DISK;
        return C_OK;
    }
    return C_OK; /* unreached */
}

/* 注意我们可能在信号处理函数 'sigShutdownHandler' 中调用此函数，
 * 因此需要保证我们调用的所有函数都是异步信号安全的。
 * 如果从信号处理函数中调用此函数，我们将不会调用
 * 非异步信号安全的 bg_unlink。 */
void rdbRemoveTempFile(pid_t childpid, int from_signal) {
    char tmpfile[256];
    char pid[32];

    /* 使用异步信号安全的函数生成临时 rdb 文件名 */
    ll2string(pid, sizeof(pid), childpid);
    redis_strlcpy(tmpfile, "temp-", sizeof(tmpfile));
    redis_strlcat(tmpfile, pid, sizeof(tmpfile));
    redis_strlcat(tmpfile, ".rdb", sizeof(tmpfile));

    if (from_signal) {
        /* bg_unlink 不是异步信号安全的，但在这种情况下
         * 我们并不真的需要关闭 fd，它会在进程退出时被释放。 */
        int fd = open(tmpfile, O_RDONLY|O_NONBLOCK);
        UNUSED(fd);
        unlink(tmpfile);
    } else {
        bg_unlink(tmpfile);
    }
}

/* 当代码处于 RDB-check 模式且发现一个无需实际模块即可解析的
 * type 2 模块值时，rdbLoadObject() 会调用此函数。
 * 该值被解析以检查错误，最后返回一个虚拟的 redis 对象
 * 以符合 API 要求。 */
robj *rdbLoadCheckModuleValue(rio *rdb, char *modulename) {
    uint64_t opcode;
    while((opcode = rdbLoadLen(rdb,NULL)) != RDB_MODULE_OPCODE_EOF) {
        if (opcode == RDB_MODULE_OPCODE_SINT ||
            opcode == RDB_MODULE_OPCODE_UINT)
        {
            uint64_t len;
            if (rdbLoadLenByRef(rdb,NULL,&len) == -1) {
                rdbReportCorruptRDB(
                    "Error reading integer from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_STRING) {
            robj *o = rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
            if (o == NULL) {
                rdbReportCorruptRDB(
                    "Error reading string from module %s value", modulename);
            }
            decrRefCount(o);
        } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
            float val;
            if (rdbLoadBinaryFloatValue(rdb,&val) == -1) {
                rdbReportCorruptRDB(
                    "Error reading float from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
            double val;
            if (rdbLoadBinaryDoubleValue(rdb,&val) == -1) {
                rdbReportCorruptRDB(
                    "Error reading double from module %s value", modulename);
            }
        }
    }
    return createStringObject("module-dummy-value",18);
}

/* hashZiplistConvertAndValidateIntegrity 的回调函数。
 * 检查 ziplist 中没有重复的哈希字段名。
 * 由 'p' 指向的 ziplist 元素将被转换并存储到 listpack 中。 */
static int _ziplistPairsEntryConvertAndValidate(unsigned char *p, unsigned int head_count, void *userdata) {
    unsigned char *str;
    unsigned int slen;
    long long vll;

    struct {
        long count;
        dict *fields;
        unsigned char **lp;
    } *data = userdata;

    if (data->fields == NULL) {
        data->fields = dictCreate(&hashDictType);
        dictExpand(data->fields, head_count/2);
    }

    if (!ziplistGet(p, &str, &slen, &vll))
        return 0;

    /* 偶数索引的记录是字段名，添加到字典并检查是否重复 */
    if (((data->count) & 1) == 0) {
        sds field = str? sdsnewlen(str, slen): sdsfromlonglong(vll);
        if (dictAdd(data->fields, field, NULL) != DICT_OK) {
            /* 重复，返回错误 */
            sdsfree(field);
            return 0;
        }
    }

    if (str) {
        *(data->lp) = lpAppend(*(data->lp), (unsigned char*)str, slen);
    } else {
        *(data->lp) = lpAppendInteger(*(data->lp), vll);
    }

    (data->count)++;
    return 1;
}

/* 在将数据结构转换为 listpack 并存储到 'lp' 的同时验证其完整性。
 * 该函数可以安全地用于未经验证的 ziplist，
 * 当遇到完整性验证问题时返回 0。 */
int ziplistPairsConvertAndValidateIntegrity(unsigned char *zl, size_t size, unsigned char **lp) {
    /* Keep track of the field names to locate duplicate ones */
    struct {
        long count;
        dict *fields; /* Initialisation at the first callback. */
        unsigned char **lp;
    } data = {0, NULL, lp};

    int ret = ziplistValidateIntegrity(zl, size, 1, _ziplistPairsEntryConvertAndValidate, &data);

    /* make sure we have an even number of records. */
    if (data.count & 1)
        ret = 0;

    if (data.fields) dictRelease(data.fields);
    return ret;
}

/* ziplistValidateIntegrity 的回调函数。
 * 由 'p' 指向的 ziplist 元素将被转换并存储到 listpack 中。 */
static int _ziplistEntryConvertAndValidate(unsigned char *p, unsigned int head_count, void *userdata) {
    UNUSED(head_count);
    unsigned char *str;
    unsigned int slen;
    long long vll;
    unsigned char **lp = (unsigned char**)userdata;

    if (!ziplistGet(p, &str, &slen, &vll)) return 0;

    if (str)
        *lp = lpAppend(*lp, (unsigned char*)str, slen);
    else
        *lp = lpAppendInteger(*lp, vll);

    return 1;
}

/* ziplistValidateIntegrity 的回调函数。
 * 由 'p' 指向的 ziplist 元素将被转换并存储到 quicklist 中。 */
static int _listZiplistEntryConvertAndValidate(unsigned char *p, unsigned int head_count, void *userdata) {
    UNUSED(head_count);
    unsigned char *str;
    unsigned int slen;
    long long vll;
    char longstr[32] = {0};
    quicklist *ql = (quicklist*)userdata;

    if (!ziplistGet(p, &str, &slen, &vll)) return 0;
    if (!str) {
        /* 将 longval 作为字符串写入以便能够重新添加 */
        slen = ll2string(longstr, sizeof(longstr), vll);
        str = (unsigned char *)longstr;
    }
    quicklistPushTail(ql, str, slen);
    return 1;
}

/* 用于检查 listpack 中没有重复记录的回调函数 */
static int _lpEntryValidation(unsigned char *p, unsigned int head_count, void *userdata) {
    struct {
        int tuple_len;
        long count;
        dict *fields;
        long long last_expireat;
    } *data = userdata;

    if (data->fields == NULL) {
        data->fields = dictCreate(&hashDictType);
        dictExpand(data->fields, head_count/data->tuple_len);
    }

    /* 如果我们在检查键值对，则偶数索引的记录是字段名。
     * 否则我们在检查所有元素。将其添加到字典并检查是否重复 */
    if (data->count % data->tuple_len == 0) {
        unsigned char *str;
        int64_t slen;
        unsigned char buf[LP_INTBUF_SIZE];

        str = lpGet(p, &slen, buf);
        sds field = sdsnewlen(str, slen);
        if (dictAdd(data->fields, field, NULL) != DICT_OK) {
            /* 重复，返回错误 */
            sdsfree(field);
            return 0;
        }
    }

    /* 仅对 listpackex 验证 TTL 字段 */
    if (data->count % data->tuple_len == 2) {
        long long expire_at;
        /* 必须是整数 */
        if (!lpGetIntegerValue(p, &expire_at)) return 0;
        /* 必须小于 EB_EXPIRE_TIME_MAX */
        if (expire_at < 0 || (unsigned long long)expire_at > EB_EXPIRE_TIME_MAX) return 0;
        /* TTL 字段是有序的。如果当前字段有 TTL，则前一个字段也必须有 TTL，
         * 且当前 TTL 必须大于前一个 TTL。 */
        if (expire_at != 0 && (data->last_expireat == 0 || expire_at < data->last_expireat)) return 0;
        data->last_expireat = expire_at;
    }

    (data->count)++;
    return 1;
}

/* 验证 listpack 结构的完整性。
 * 当 `deep` 为 0 时，仅验证头部的完整性。
 * 当 `deep` 为 1 时，我们会逐个扫描所有条目。
 * tuple_len 表示一个逻辑条目的元组大小。
 * 无论 tuple 大小是 1（集合）、2（field-value）还是 3（field-value[-ttl]），
 * 元组中的第一个元素必须是唯一的。 */
int lpValidateIntegrityAndDups(unsigned char *lp, size_t size, int deep, int tuple_len) {
    if (!deep)
        return lpValidateIntegrity(lp, size, 0, NULL, NULL);

    /* Keep track of the field names to locate duplicate ones */
    struct {
        int tuple_len;
        long count;
        dict *fields; /* Initialisation at the first callback. */
        long long last_expireat; /* Last field's expiry time to ensure order in TTL fields. */
    } data = {tuple_len, 0, NULL, -1};

    int ret = lpValidateIntegrity(lp, size, 1, _lpEntryValidation, &data);

    /* the number of records should be a multiple of the tuple length */
    if (data.count % tuple_len != 0)
        ret = 0;

    if (data.fields) dictRelease(data.fields);
    return ret;
}

/* 从指定文件加载指定类型的 Redis 对象。
 * 成功时返回一个新分配的对象，否则返回 NULL。
 *
 * error - 当函数返回 NULL 且 'error' 不为 NULL 时，
 *   'error' 指向的整数被设置为发生的错误类型。
 * minExpiredField - 如果加载的是带字段过期的哈希，
 *   则此值被设置为哈希字段中找到的最小过期时间。
 *   如果没有带过期的字段，或者不是哈希，则设置为 EB_EXPIRE_TIME_INVALID。
 */
robj *rdbLoadObject(int rdbtype, rio *rdb, sds key, int dbid, int *error)
{
    robj *o = NULL, *ele, *dec;
    uint64_t len;
    unsigned int i;

    /* 设置加载对象的默认错误，成功时将被设置为 0 */
    if (error) *error = RDB_LOAD_ERR_OTHER;

    int deep_integrity_validation = server.sanitize_dump_payload == SANITIZE_DUMP_YES;
    if (server.sanitize_dump_payload == SANITIZE_DUMP_CLIENTS) {
        /* 在加载（RDB）时，或从主节点或具有 skip-sanitize-payload 标志的
         * ACL 用户的客户端接收到 RESTORE 命令时，跳过清理 */
        int skip = server.loading ||
            (server.current_client && (server.current_client->flags & CLIENT_MASTER));
        if (!skip && server.current_client && server.current_client->user)
            skip = !!(server.current_client->user->flags & USER_FLAG_SANITIZE_PAYLOAD_SKIP);
        deep_integrity_validation = !skip;
    }

    if (rdbtype == RDB_TYPE_STRING) {
        /* 读取字符串值 */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncodingEx(o, 0);
    } else if (rdbtype == RDB_TYPE_LIST) {
        /* 读取列表值 */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        if (len == 0) goto emptykey;

        o = createQuicklistObject(server.list_max_listpack_size, server.list_compress_depth);

        /* 加载列表中的每个元素 */
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) {
                decrRefCount(o);
                return NULL;
            }
            dec = getDecodedObject(ele);
            size_t len = sdslen(dec->ptr);
            quicklistPushTail(o->ptr, dec->ptr, len);
            decrRefCount(dec);
            decrRefCount(ele);
        }

        listTypeTryConversion(o, LIST_CONV_AUTO, NULL, NULL);
    } else if (rdbtype == RDB_TYPE_SET) {
        /* 读取 Set 值 */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        if (len == 0) goto emptykey;

        /* 当条目过多时使用常规集合 */
        size_t max_entries = server.set_max_intset_entries;
        if (max_entries >= 1<<30) max_entries = 1<<30;
        if (len > max_entries) {
            o = createSetObject();
            /* 尽快将 dict 扩展到合适的大小可以加快速度，
             * 以避免 rehash */
            if (len > DICT_HT_INITIAL_SIZE && dictTryExpand(o->ptr, len) != DICT_OK) {
                rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                decrRefCount(o);
                return NULL;
            }
        } else {
            o = createIntsetObject();
        }

        /* 加载集合中的每个元素 */
        size_t maxelelen = 0, sumelelen = 0;
        for (i = 0; i < len; i++) {
            long long llval;
            sds sdsele;

            if ((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }
            size_t elelen = sdslen(sdsele);
            sumelelen += elelen;
            if (elelen > maxelelen) maxelelen = elelen;

            if (o->encoding == OBJ_ENCODING_INTSET) {
                /* 从元素获取整数值 */
                if (isSdsRepresentableAsLongLong(sdsele,&llval) == C_OK) {
                    uint8_t success;
                    o->ptr = intsetAdd(o->ptr, llval, &success);
                    if (!success) {
                        rdbReportCorruptRDB("Duplicate set members detected");
                        decrRefCount(o);
                        sdsfree(sdsele);
                        return NULL;
                    }
                } else if (setTypeSize(o) < server.set_max_listpack_entries &&
                           maxelelen <= server.set_max_listpack_value &&
                           lpSafeToAdd(NULL, sumelelen))
                {
                    /* 我们检查了添加一个大元素是否比添加多个小元素更安全。
                     * 这样做没问题，因为 lpSafeToAdd 不关心单个元素，
                     * 仅关心总大小。 */
                    setTypeConvert(o, OBJ_ENCODING_LISTPACK);
                } else if (setTypeConvertAndExpand(o, OBJ_ENCODING_HT, len, 0) != C_OK) {
                    rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                    sdsfree(sdsele);
                    decrRefCount(o);
                    return NULL;
                }
            }

            /* 当集合刚刚被转换为 listpack 编码的集合时也会执行此处 */
            if (o->encoding == OBJ_ENCODING_LISTPACK) {
                if (setTypeSize(o) < server.set_max_listpack_entries &&
                    elelen <= server.set_max_listpack_value &&
                    lpSafeToAdd(o->ptr, elelen))
                {
                    unsigned char *p = lpFirst(o->ptr);
                    if (p && lpFind(o->ptr, p, (unsigned char*)sdsele, elelen, 0)) {
                        rdbReportCorruptRDB("Duplicate set members detected");
                        decrRefCount(o);
                        sdsfree(sdsele);
                        return NULL;
                    }
                    o->ptr = lpAppend(o->ptr, (unsigned char *)sdsele, elelen);
                } else if (setTypeConvertAndExpand(o, OBJ_ENCODING_HT, len, 0) != C_OK) {
                    rdbReportCorruptRDB("OOM in dictTryExpand %llu",
                                        (unsigned long long)len);
                    sdsfree(sdsele);
                    decrRefCount(o);
                    return NULL;
                }
            }

            /* 当集合刚刚被转换为常规哈希表编码的集合时也会执行此处 */
            if (o->encoding == OBJ_ENCODING_HT) {
                if (dictAdd((dict*)o->ptr, sdsele, NULL) != DICT_OK) {
                    rdbReportCorruptRDB("Duplicate set members detected");
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            } else {
                sdsfree(sdsele);
            }
        }
    } else if (rdbtype == RDB_TYPE_ZSET_2 || rdbtype == RDB_TYPE_ZSET) {
        /* 读取有序集合值 */
        uint64_t zsetlen;
        size_t maxelelen = 0, totelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        if (zsetlen == 0) goto emptykey;

        o = createZsetObject();
        zs = o->ptr;

        if (zsetlen > DICT_HT_INITIAL_SIZE && dictTryExpand(zs->dict,zsetlen) != DICT_OK) {
            rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)zsetlen);
            decrRefCount(o);
            return NULL;
        }

        /* 加载有序集合中的每个元素 */
        while(zsetlen--) {
            sds sdsele;
            double score;
            zskiplistNode *znode;

            if ((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }

            if (rdbtype == RDB_TYPE_ZSET_2) {
                if (rdbLoadBinaryDoubleValue(rdb,&score) == -1) {
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            } else {
                if (rdbLoadDoubleValue(rdb,&score) == -1) {
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            }

            if (isnan(score)) {
                rdbReportCorruptRDB("Zset with NAN score detected");
                decrRefCount(o);
                sdsfree(sdsele);
                return NULL;
            }

            /* 不关心整数编码的字符串 */
            if (sdslen(sdsele) > maxelelen) maxelelen = sdslen(sdsele);
            totelelen += sdslen(sdsele);

            znode = zslInsert(zs->zsl,score,sdsele);
            if (dictAdd(zs->dict,sdsele,&znode->score) != DICT_OK) {
                rdbReportCorruptRDB("Duplicate zset fields detected");
                decrRefCount(o);
                /* 无需释放 'sdsele'，它会与 'o' 一起通过 zslFree 释放 */
                return NULL;
            }
        }

        /* 在加载完成之后再进行转换，因为有序集合并非有序存储 */
        if (zsetLength(o) <= server.zset_max_listpack_entries &&
            maxelelen <= server.zset_max_listpack_value &&
            lpSafeToAdd(NULL, totelelen))
        {
            zsetConvert(o, OBJ_ENCODING_LISTPACK);
        }
    } else if (rdbtype == RDB_TYPE_HASH) {
        uint64_t len;
        int ret;
        sds value;
        hfield field;
        dict *dupSearchDict = NULL;

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;
        if (len == 0) goto emptykey;

        o = createHashObject();

        /* 条目过多？从一开始就使用哈希表 */
        if (len > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
        else if (deep_integrity_validation) {
            /* 在此模式下，我们需要保证稍后 ziplist 转换为 dict 时
             * 服务器不会崩溃。
             * 创建一个集合（不包含值的 dict）用于查找重复。
             * 一旦将 ziplist 转换为哈希，就可以丢弃它。 */
            dupSearchDict = dictCreate(&hashDictType);
        }

        /* 将每个字段和值加载到 listpack 中 */
        while (o->encoding == OBJ_ENCODING_LISTPACK && len > 0) {
            len--;
            /* 加载原始字符串 */
            if ((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_HFLD,NULL)) == NULL) {
                decrRefCount(o);
                if (dupSearchDict) dictRelease(dupSearchDict);
                return NULL;
            }
            if ((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                hfieldFree(field);
                decrRefCount(o);
                if (dupSearchDict) dictRelease(dupSearchDict);
                return NULL;
            }

            if (dupSearchDict) {
                sds field_dup = sdsnewlen(field, hfieldlen(field));

                if (dictAdd(dupSearchDict, field_dup, NULL) != DICT_OK) {
                    rdbReportCorruptRDB("Hash with dup elements");
                    dictRelease(dupSearchDict);
                    decrRefCount(o);
                    sdsfree(field_dup);
                    hfieldFree(field);
                    sdsfree(value);
                    return NULL;
                }
            }

            /* 超过大小阈值时转换为哈希表 */
            if (hfieldlen(field) > server.hash_max_listpack_value ||
                sdslen(value) > server.hash_max_listpack_value ||
                !lpSafeToAdd(o->ptr, hfieldlen(field) + sdslen(value)))
            {
                hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
                dictUseStoredKeyApi((dict *)o->ptr, 1);
                ret = dictAdd((dict*)o->ptr, field, value);
                dictUseStoredKeyApi((dict *)o->ptr, 0);
                if (ret == DICT_ERR) {
                    rdbReportCorruptRDB("Duplicate hash fields detected");
                    if (dupSearchDict) dictRelease(dupSearchDict);
                    sdsfree(value);
                    hfieldFree(field);
                    decrRefCount(o);
                    return NULL;
                }
                break;
            }

            /* 将键值对添加到 listpack */
            o->ptr = lpAppend(o->ptr, (unsigned char*)field, hfieldlen(field));
            o->ptr = lpAppend(o->ptr, (unsigned char*)value, sdslen(value));

            hfieldFree(field);
            sdsfree(value);
        }

        if (dupSearchDict) {
            /* 我们不再需要它，从现在起条目会被添加到 dict 中，
             * 因此该检查是隐式执行的。 */
            dictRelease(dupSearchDict);
            dupSearchDict = NULL;
        }

        if (o->encoding == OBJ_ENCODING_HT && len > DICT_HT_INITIAL_SIZE) {
            if (dictTryExpand(o->ptr, len) != DICT_OK) {
                rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                decrRefCount(o);
                return NULL;
            }
        }

        /* 将剩余的字段和值加载到哈希表中 */
        while (o->encoding == OBJ_ENCODING_HT && len > 0) {
            len--;
            /* 加载编码字符串 */
            if ((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_HFLD,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }
            if ((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                hfieldFree(field);
                decrRefCount(o);
                return NULL;
            }

            /* 将键值对添加到哈希表 */
            dict *d = o->ptr;
            dictUseStoredKeyApi(d, 1);
            ret = dictAdd(d, field, value);
            dictUseStoredKeyApi(d, 0);
            if (ret == DICT_ERR) {
                rdbReportCorruptRDB("Duplicate hash fields detected");
                sdsfree(value);
                hfieldFree(field);
                decrRefCount(o);
                return NULL;
            }
        }

        /* 此时应该已读取所有键值对 */
        serverAssert(len == 0);
    } else if (rdbtype == RDB_TYPE_HASH_METADATA || rdbtype == RDB_TYPE_HASH_METADATA_PRE_GA) {
        sds value;
        hfield field;
        uint64_t ttl, expireAt, minExpire = EB_EXPIRE_TIME_INVALID;
        dict *dupSearchDict = NULL;

        /* 如果是带 TTL 的哈希，加载下一个/最小过期时间
         *
         * - 此值被序列化以便将来用于直接将对象流式传输到 FLASH
         *   的用例（同时在内存中保留其下一个过期时间）。
         * - 它也用于在 RDB 文件中仅为字段保留相对 TTL。
         */
        if (rdbtype == RDB_TYPE_HASH_METADATA) {
            minExpire = rdbLoadMillisecondTime(rdb, RDB_VERSION);
            if (rioGetReadError(rdb)) {
                rdbReportCorruptRDB("Hash failed loading minExpire");
                return NULL;
            }
            if (minExpire > EB_EXPIRE_TIME_INVALID) {
                rdbReportCorruptRDB("Hash read invalid minExpire value");
            }
        }

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;
        if (len == 0) goto emptykey;
        /* TODO: 直接创建 listpackEx 或 HT */
        o = createHashObject();
        /* 条目过多？从一开始就使用哈希表 */
        if (len > server.hash_max_listpack_entries) {
            hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
            dictTypeAddMeta((dict**)&o->ptr, &mstrHashDictTypeWithHFE);
            initDictExpireMetadata(key, o);
        } else {
            hashTypeConvert(o, OBJ_ENCODING_LISTPACK_EX, NULL);
            if (deep_integrity_validation) {
                /* 在此模式下，我们需要保证稍后 listpack 转换为 dict 时
                 * 服务器不会崩溃。
                 * 创建一个集合（不包含值的 dict）用于查找重复。
                 * 一旦将 listpack 转换为哈希，就可以丢弃它。 */
                dupSearchDict = dictCreate(&hashDictType);
            }
        }

        while (len > 0) {
            len--;

            /* 读取 TTL */
            if (rdbLoadLenByRef(rdb, NULL, &ttl) == -1) {
                serverLog(LL_WARNING, "failed reading hash TTL");
                decrRefCount(o);
                if (dupSearchDict != NULL) dictRelease(dupSearchDict);
                return NULL;
            }


            if (rdbtype == RDB_TYPE_HASH_METADATA) {
                /* 已加载的 TTL 值：
                 *  - 0：表示无 TTL。这是常见情况，所以保持较小值。
                 *  - 其他值：TTL 相对于 minExpire（+1 以避免与已使用的 0 冲突）
                 */
                expireAt = (ttl != 0) ? (ttl + minExpire - 1) : 0;
            } else { /* RDB_TYPE_HASH_METADATA_PRE_GA */
                expireAt = ttl; /* 值为绝对值 */
            }

            if (expireAt > EB_EXPIRE_TIME_MAX) {
                rdbReportCorruptRDB("invalid expireAt time: %llu",
                                    (unsigned long long) expireAt);
                decrRefCount(o);
                if (dupSearchDict != NULL) dictRelease(dupSearchDict);
                return NULL;
            }

            /* 如果需要，创建带 TTL 元数据的字段 */
            if (expireAt !=0)
                field = rdbGenericLoadStringObject(rdb, RDB_LOAD_HFLD_TTL, NULL);
            else
                field = rdbGenericLoadStringObject(rdb, RDB_LOAD_HFLD, NULL);

            if (field == NULL) {
                serverLog(LL_WARNING, "failed reading hash field");
                decrRefCount(o);
                if (dupSearchDict != NULL) dictRelease(dupSearchDict);
                return NULL;
            }

            /* 读取值 */
            if ((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                serverLog(LL_WARNING, "failed reading hash value");
                decrRefCount(o);
                if (dupSearchDict != NULL) dictRelease(dupSearchDict);
                hfieldFree(field);
                return NULL;
            }

            /* 存储读取的值 - 存储到 listpack 或 dict */
            if (o->encoding == OBJ_ENCODING_LISTPACK_EX) {
                /* 完整性 - 检查键是否重复（如果需要） */
                if (dupSearchDict) {
                    sds field_dup = sdsnewlen(field, hfieldlen(field));

                    if (dictAdd(dupSearchDict, field_dup, NULL) != DICT_OK) {
                        rdbReportCorruptRDB("Hash with dup elements");
                        dictRelease(dupSearchDict);
                        decrRefCount(o);
                        sdsfree(field_dup);
                        sdsfree(value);
                        hfieldFree(field);
                        return NULL;
                    }
                }

                /* 检查值是否能够保存到 listpack（或应转换为 dict 编码） */
                if (hfieldlen(field) > server.hash_max_listpack_value ||
                    sdslen(value) > server.hash_max_listpack_value ||
                    !lpSafeToAdd(((listpackEx*)o->ptr)->lp, hfieldlen(field) + sdslen(value) + lpEntrySizeInteger(expireAt)))
                {
                    /* 转换为哈希 */
                    hashTypeConvert(o, OBJ_ENCODING_HT, NULL);

                    if (len > DICT_HT_INITIAL_SIZE) { /* TODO: 这并非原始 len，但简单哈希也存在这种情况，这是一个 bug 吗？ */
                        if (dictTryExpand(o->ptr, len) != DICT_OK) {
                            rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                            decrRefCount(o);
                            if (dupSearchDict != NULL) dictRelease(dupSearchDict);
                            sdsfree(value);
                            hfieldFree(field);
                            return NULL;
                        }
                    }

                    /* 不要将值添加到新哈希中：下一个 if 会捕获并在相应位置添加这些值 */
                } else {
                    listpackExAddNew(o, field, hfieldlen(field),
                                     value, sdslen(value), expireAt);
                    hfieldFree(field);
                    sdsfree(value);
                }
            }

            if (o->encoding == OBJ_ENCODING_HT) {
                /* 将键值对添加到哈希表 */
                dict *d = o->ptr;
                dictUseStoredKeyApi(d, 1);
                int ret = dictAdd(d, field, value);
                dictUseStoredKeyApi(d, 0);

                /* 将过期时间附加到哈希字段，并在哈希私有 HFE DS 中注册 */
                if ((ret != DICT_ERR) && expireAt) {
                    dictExpireMetadata *m = (dictExpireMetadata *) dictMetadata(d);
                    ret = ebAdd(&m->hfe, &hashFieldExpireBucketsType, field, expireAt);
                }

                if (ret == DICT_ERR) {
                    rdbReportCorruptRDB("Duplicate hash fields detected");
                    sdsfree(value);
                    hfieldFree(field);
                    decrRefCount(o);
                    return NULL;
                }
            }
        }

        if (dupSearchDict != NULL) dictRelease(dupSearchDict);

    } else if (rdbtype == RDB_TYPE_LIST_QUICKLIST || rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        if (len == 0) goto emptykey;

        o = createQuicklistObject(server.list_max_listpack_size, server.list_compress_depth);
        uint64_t container = QUICKLIST_NODE_CONTAINER_PACKED;
        while (len--) {
            unsigned char *lp;
            size_t encoded_len;

            if (rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
                if ((container = rdbLoadLen(rdb,NULL)) == RDB_LENERR) {
                    decrRefCount(o);
                    return NULL;
                }

                if (container != QUICKLIST_NODE_CONTAINER_PACKED && container != QUICKLIST_NODE_CONTAINER_PLAIN) {
                    rdbReportCorruptRDB("Quicklist integrity check failed.");
                    decrRefCount(o);
                    return NULL;
                }
            }

            unsigned char *data =
                rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&encoded_len);
            if (data == NULL || (encoded_len == 0)) {
                zfree(data);
                decrRefCount(o);
                return NULL;
            }

            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                quicklistAppendPlainNode(o->ptr, data, encoded_len);
                continue;
            }

            if (rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
                lp = data;
                if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
                if (!lpValidateIntegrity(lp, encoded_len, deep_integrity_validation, NULL, NULL)) {
                    rdbReportCorruptRDB("Listpack integrity check failed.");
                    decrRefCount(o);
                    zfree(lp);
                    return NULL;
                }
            } else {
                lp = lpNew(encoded_len);
                if (!ziplistValidateIntegrity(data, encoded_len, 1,
                        _ziplistEntryConvertAndValidate, &lp))
                {
                    rdbReportCorruptRDB("Ziplist integrity check failed.");
                    decrRefCount(o);
                    zfree(data);
                    zfree(lp);
                    return NULL;
                }
                zfree(data);
                lp = lpShrinkToFit(lp);
            }

            /* Silently skip empty ziplists, if we'll end up with empty quicklist we'll fail later. */
            if (lpLength(lp) == 0) {
                zfree(lp);
                continue;
            } else {
                quicklistAppendListpack(o->ptr, lp);
            }
        }

        if (quicklistCount(o->ptr) == 0) {
            decrRefCount(o);
            goto emptykey;
        }

        listTypeTryConversion(o, LIST_CONV_AUTO, NULL, NULL);
    } else if (rdbtype == RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == RDB_TYPE_SET_INTSET   ||
               rdbtype == RDB_TYPE_SET_LISTPACK ||
               rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == RDB_TYPE_ZSET_LISTPACK ||
               rdbtype == RDB_TYPE_HASH_ZIPLIST ||
               rdbtype == RDB_TYPE_HASH_LISTPACK ||
               rdbtype == RDB_TYPE_HASH_LISTPACK_EX_PRE_GA ||
               rdbtype == RDB_TYPE_HASH_LISTPACK_EX)
    {
        size_t encoded_len;

        /* 对于带 TTL 的哈希，在加载编码数据前先加载最小过期时间 */
        if (rdbtype == RDB_TYPE_HASH_LISTPACK_EX) {
            uint64_t minExpire = rdbLoadMillisecondTime(rdb, RDB_VERSION);
            /* 此值序列化用于未来将对象直接流式传输到 FLASH
             * （同时在内存中保留其下一个过期时间）的用例 */
            UNUSED(minExpire);
            if (rioGetReadError(rdb)) {
                rdbReportCorruptRDB( "Hash listpackex integrity check failed.");
                return NULL;
            }
        }

        unsigned char *encoded =
            rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&encoded_len);
        if (encoded == NULL) return NULL;

        o = createObject(OBJ_STRING, encoded); /* Obj type fixed below. */

        /* 修正对象编码，如果根据当前配置编码数据类型中
         * 包含过多元素，则将其转换为基本类型。
         * 注意我们只检查长度而不检查最大元素大小，
         * 因为这是 O(N) 扫描。最终一切都会被转换。 */
        switch(rdbtype) {
            case RDB_TYPE_HASH_ZIPMAP:
                /* 由于不再保留 zipmap，加载时已经是 O(n)，
                 * 使用 'deep' 验证。 */
                if (!zipmapValidateIntegrity(encoded, encoded_len, 1)) {
                    rdbReportCorruptRDB("Zipmap integrity check failed.");
                    zfree(encoded);
                    o->ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                /* 转换为 ziplist 编码的哈希。当加载
                 * Redis 2.4 创建的转储被弃用时，此处也应弃用。 */
                {
                    unsigned char *lp = lpNew(0);
                    unsigned char *zi = zipmapRewind(o->ptr);
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;
                    dict *dupSearchDict = dictCreate(&hashDictType);

                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;

                        /* 搜索重复记录 */
                        sds field = sdstrynewlen(fstr, flen);
                        if (!field || dictAdd(dupSearchDict, field, NULL) != DICT_OK ||
                            !lpSafeToAdd(lp, (size_t)flen + vlen)) {
                            rdbReportCorruptRDB("Hash zipmap with dup elements, or big length (%u)", flen);
                            dictRelease(dupSearchDict);
                            sdsfree(field);
                            zfree(encoded);
                            o->ptr = NULL;
                            decrRefCount(o);
                            return NULL;
                        }

                        lp = lpAppend(lp, fstr, flen);
                        lp = lpAppend(lp, vstr, vlen);
                    }

                    dictRelease(dupSearchDict);
                    zfree(o->ptr);
                    o->ptr = lp;
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_LISTPACK;

                    if (hashTypeLength(o, 0) > server.hash_max_listpack_entries ||
                        maxlen > server.hash_max_listpack_value)
                    {
                        hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
                    }
                }
                break;
            case RDB_TYPE_LIST_ZIPLIST:
                {
                    quicklist *ql = quicklistNew(server.list_max_listpack_size,
                                                 server.list_compress_depth);

                    if (!ziplistValidateIntegrity(encoded, encoded_len, 1,
                            _listZiplistEntryConvertAndValidate, ql))
                    {
                        rdbReportCorruptRDB("List ziplist integrity check failed.");
                        zfree(encoded);
                        o->ptr = NULL;
                        decrRefCount(o);
                        quicklistRelease(ql);
                        return NULL;
                    }

                    if (ql->len == 0) {
                        zfree(encoded);
                        o->ptr = NULL;
                        decrRefCount(o);
                        quicklistRelease(ql);
                        goto emptykey;
                    }

                    zfree(encoded);
                    o->type = OBJ_LIST;
                    o->ptr = ql;
                    o->encoding = OBJ_ENCODING_QUICKLIST;
                    break;
                }
            case RDB_TYPE_SET_INTSET:
                if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
                if (!intsetValidateIntegrity(encoded, encoded_len, deep_integrity_validation)) {
                    rdbReportCorruptRDB("Intset integrity check failed.");
                    zfree(encoded);
                    o->ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_INTSET;
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o, OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_SET_LISTPACK:
                if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
                if (!lpValidateIntegrityAndDups(encoded, encoded_len, deep_integrity_validation, 1)) {
                    rdbReportCorruptRDB("Set listpack integrity check failed.");
                    zfree(encoded);
                    o->ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_LISTPACK;

                if (setTypeSize(o) == 0) {
                    zfree(encoded);
                    o->ptr = NULL;
                    decrRefCount(o);
                    goto emptykey;
                }
                if (setTypeSize(o) > server.set_max_listpack_entries)
                    setTypeConvert(o, OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_ZSET_ZIPLIST:
                {
                    unsigned char *lp = lpNew(encoded_len);
                    if (!ziplistPairsConvertAndValidateIntegrity(encoded, encoded_len, &lp)) {
                        rdbReportCorruptRDB("Zset ziplist integrity check failed.");
                        zfree(lp);
                        zfree(encoded);
                        o->ptr = NULL;
                        decrRefCount(o);
                        return NULL;
                    }

                    zfree(o->ptr);
                    o->type = OBJ_ZSET;
                    o->ptr = lp;
                    o->encoding = OBJ_ENCODING_LISTPACK;
                    if (zsetLength(o) == 0) {
                        decrRefCount(o);
                        goto emptykey;
                    }

                    if (zsetLength(o) > server.zset_max_listpack_entries)
                        zsetConvert(o, OBJ_ENCODING_SKIPLIST);
                    else
                        o->ptr = lpShrinkToFit(o->ptr);
                    break;
                }
            case RDB_TYPE_ZSET_LISTPACK:
                if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
                if (!lpValidateIntegrityAndDups(encoded, encoded_len, deep_integrity_validation, 2)) {
                    rdbReportCorruptRDB("Zset listpack integrity check failed.");
                    zfree(encoded);
                    o->ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_ZSET;
                o->encoding = OBJ_ENCODING_LISTPACK;
                if (zsetLength(o) == 0) {
                    decrRefCount(o);
                    goto emptykey;
                }

                if (zsetLength(o) > server.zset_max_listpack_entries)
                    zsetConvert(o, OBJ_ENCODING_SKIPLIST);
                break;
            case RDB_TYPE_HASH_ZIPLIST:
                {
                    unsigned char *lp = lpNew(encoded_len);
                    if (!ziplistPairsConvertAndValidateIntegrity(encoded, encoded_len, &lp)) {
                        rdbReportCorruptRDB("Hash ziplist integrity check failed.");
                        zfree(lp);
                        zfree(encoded);
                        o->ptr = NULL;
                        decrRefCount(o);
                        return NULL;
                    }

                    zfree(o->ptr);
                    o->ptr = lp;
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_LISTPACK;
                    if (hashTypeLength(o, 0) == 0) {
                        decrRefCount(o);
                        goto emptykey;
                    }

                    if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
                        hashTypeConvert(o, OBJ_ENCODING_HT, NULL);
                    else
                        o->ptr = lpShrinkToFit(o->ptr);
                    break;
                }
            case RDB_TYPE_HASH_LISTPACK:
            case RDB_TYPE_HASH_LISTPACK_EX_PRE_GA:
            case RDB_TYPE_HASH_LISTPACK_EX:
                /* 带 TTL 的 listpack 编码哈希需要自己的结构，
                 * 通过 o->ptr 指向 */
                o->type = OBJ_HASH;
                if ( (rdbtype == RDB_TYPE_HASH_LISTPACK_EX) ||
                     (rdbtype == RDB_TYPE_HASH_LISTPACK_EX_PRE_GA) ) {
                    listpackEx *lpt = listpackExCreate();
                    lpt->lp = encoded;
                    lpt->key = key;
                    o->ptr = lpt;
                    o->encoding = OBJ_ENCODING_LISTPACK_EX;
                } else
                    o->encoding = OBJ_ENCODING_LISTPACK;

                /* tuple_len 是每个键的元素数量：
                 * 简单哈希为 key + value，带 TTL 的哈希为 key + value + ttl */
                int tuple_len = (rdbtype == RDB_TYPE_HASH_LISTPACK ? 2 : 3);
                /* 验证读取的数据 */
                if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
                if (!lpValidateIntegrityAndDups(encoded, encoded_len,
                                                deep_integrity_validation, tuple_len)) {
                    rdbReportCorruptRDB("Hash listpack integrity check failed.");
                    decrRefCount(o);
                    return NULL;
                }

                /* 如果 listpack 为空，则删除它 */
                if (hashTypeLength(o, 0) == 0) {
                    decrRefCount(o);
                    goto emptykey;
                }

                /* 如果有 HFE，将 listpack 转换为哈希表但不注册到全局 HFE DS，
                 * 因为此时 listpack 还未连接到 DB */
                if (hashTypeLength(o, 0) > server.hash_max_listpack_entries)
                    hashTypeConvert(o, OBJ_ENCODING_HT, NULL /*db->hexpires*/);

                break;
            default:
                /* 完全不可达 */
                rdbReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
                break;
        }
    } else if (rdbtype == RDB_TYPE_STREAM_LISTPACKS ||
               rdbtype == RDB_TYPE_STREAM_LISTPACKS_2 ||
               rdbtype == RDB_TYPE_STREAM_LISTPACKS_3)
    {
        o = createStreamObject();
        stream *s = o->ptr;
        uint64_t listpacks = rdbLoadLen(rdb,NULL);
        if (listpacks == RDB_LENERR) {
            rdbReportReadError("Stream listpacks len loading failed.");
            decrRefCount(o);
            return NULL;
        }

        while(listpacks--) {
            /* 获取主 ID，用作 radix tree 节点的键：
             * listpack 内部的条目相对于该 ID 进行差分编码。 */
            sds nodekey = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
            if (nodekey == NULL) {
                rdbReportReadError("Stream master ID loading failed: invalid encoding or I/O error.");
                decrRefCount(o);
                return NULL;
            }
            if (sdslen(nodekey) != sizeof(streamID)) {
                rdbReportCorruptRDB("Stream node key entry is not the "
                                        "size of a stream ID");
                sdsfree(nodekey);
                decrRefCount(o);
                return NULL;
            }

            /* 加载 listpack */
            size_t lp_size;
            unsigned char *lp =
                rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&lp_size);
            if (lp == NULL) {
                rdbReportReadError("Stream listpacks loading failed.");
                sdsfree(nodekey);
                decrRefCount(o);
                return NULL;
            }
            if (deep_integrity_validation) server.stat_dump_payload_sanitizations++;
            if (!streamValidateListpackIntegrity(lp, lp_size, deep_integrity_validation)) {
                rdbReportCorruptRDB("Stream listpack integrity check failed.");
                sdsfree(nodekey);
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }

            unsigned char *first = lpFirst(lp);
            if (first == NULL) {
                /* 序列化后的 listpack 不应为空，因为在删除时
                 * 如果 listpack 结果为空，我们应该删除 radix tree 键。 */
                rdbReportCorruptRDB("Empty listpack inside stream");
                sdsfree(nodekey);
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }

            /* 将键插入到 radix tree 中 */
            int retval = raxTryInsert(s->rax,
                (unsigned char*)nodekey,sizeof(streamID),lp,NULL);
            sdsfree(nodekey);
            if (!retval) {
                rdbReportCorruptRDB("Listpack re-added with existing key");
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }
        }
        /* 加载 stream 中的条目总数 */
        s->length = rdbLoadLen(rdb,NULL);

        /* 加载最后一个条目 ID */
        s->last_id.ms = rdbLoadLen(rdb,NULL);
        s->last_id.seq = rdbLoadLen(rdb,NULL);

        if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_2) {
            /* 加载第一个条目 ID */
            s->first_id.ms = rdbLoadLen(rdb,NULL);
            s->first_id.seq = rdbLoadLen(rdb,NULL);

            /* 加载最大已删除条目 ID */
            s->max_deleted_entry_id.ms = rdbLoadLen(rdb,NULL);
            s->max_deleted_entry_id.seq = rdbLoadLen(rdb,NULL);

            /* 加载 offset */
            s->entries_added = rdbLoadLen(rdb,NULL);
        } else {
            /* 在迁移过程中，offset 可以初始化为 stream 的长度。
             * 此时我们也无需关心墓碑，
             * 因为 CG offset 稍后也会被初始化。 */
            s->max_deleted_entry_id.ms = 0;
            s->max_deleted_entry_id.seq = 0;
            s->entries_added = s->length;

            /* 由于 rax 已经加载，我们可以找到第一个条目的 ID。 */
            streamGetEdgeID(s,1,1,&s->first_id);
        }

        if (rioGetReadError(rdb)) {
            rdbReportReadError("Stream object metadata loading failed.");
            decrRefCount(o);
            return NULL;
        }

        if (s->length && !raxSize(s->rax)) {
            rdbReportCorruptRDB("Stream length inconsistent with rax entries");
            decrRefCount(o);
            return NULL;
        }

        /* 加载消费者组 */
        uint64_t cgroups_count = rdbLoadLen(rdb,NULL);
        if (cgroups_count == RDB_LENERR) {
            rdbReportReadError("Stream cgroup count loading failed.");
            decrRefCount(o);
            return NULL;
        }
        while(cgroups_count--) {
            /* 获取消费者组名称和 ID。然后我们可以尽快创建消费者组，
             * 并在读取更多数据时填充其结构。 */
            streamID cg_id;
            sds cgname = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
            if (cgname == NULL) {
                rdbReportReadError(
                    "Error reading the consumer group name from Stream");
                decrRefCount(o);
                return NULL;
            }

            cg_id.ms = rdbLoadLen(rdb,NULL);
            cg_id.seq = rdbLoadLen(rdb,NULL);
            if (rioGetReadError(rdb)) {
                rdbReportReadError("Stream cgroup ID loading failed.");
                sdsfree(cgname);
                decrRefCount(o);
                return NULL;
            }

            /* 加载消费者组 offset */
            uint64_t cg_offset;
            if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_2) {
                cg_offset = rdbLoadLen(rdb,NULL);
                if (rioGetReadError(rdb)) {
                    rdbReportReadError("Stream cgroup offset loading failed.");
                    sdsfree(cgname);
                    decrRefCount(o);
                    return NULL;
                }
            } else {
                cg_offset = streamEstimateDistanceFromFirstEverEntry(s,&cg_id);
            }

            streamCG *cgroup = streamCreateCG(s,cgname,sdslen(cgname),&cg_id,cg_offset);
            if (cgroup == NULL) {
                rdbReportCorruptRDB("Duplicated consumer group name %s",
                                         cgname);
                decrRefCount(o);
                sdsfree(cgname);
                return NULL;
            }
            sdsfree(cgname);

            /* 加载此消费者组的全局 PEL，但我们暂不会用消息所有者
             * 填充 NACK 结构，因为该组的消费者及其消息将在下一步读取。
             * 所以现在让它们保持未解析状态，之后再填充。 */
            uint64_t pel_size = rdbLoadLen(rdb,NULL);
            if (pel_size == RDB_LENERR) {
                rdbReportReadError("Stream PEL size loading failed.");
                decrRefCount(o);
                return NULL;
            }
            while(pel_size--) {
                unsigned char rawid[sizeof(streamID)];
                if (rioRead(rdb,rawid,sizeof(rawid)) == 0) {
                    rdbReportReadError("Stream PEL ID loading failed.");
                    decrRefCount(o);
                    return NULL;
                }
                streamNACK *nack = streamCreateNACK(NULL);
                nack->delivery_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                nack->delivery_count = rdbLoadLen(rdb,NULL);
                if (rioGetReadError(rdb)) {
                    rdbReportReadError("Stream PEL NACK loading failed.");
                    decrRefCount(o);
                    streamFreeNACK(nack);
                    return NULL;
                }
                if (!raxTryInsert(cgroup->pel,rawid,sizeof(rawid),nack,NULL)) {
                    rdbReportCorruptRDB("Duplicated global PEL entry "
                                            "loading stream consumer group");
                    decrRefCount(o);
                    streamFreeNACK(nack);
                    return NULL;
                }
            }

            /* 现在我们已经加载了全局 PEL，接下来需要加载
             * 消费者及其本地 PEL。 */
            uint64_t consumers_num = rdbLoadLen(rdb,NULL);
            if (consumers_num == RDB_LENERR) {
                rdbReportReadError("Stream consumers num loading failed.");
                decrRefCount(o);
                return NULL;
            }
            while(consumers_num--) {
                sds cname = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
                if (cname == NULL) {
                    rdbReportReadError(
                        "Error reading the consumer name from Stream group.");
                    decrRefCount(o);
                    return NULL;
                }
                streamConsumer *consumer = streamCreateConsumer(cgroup,cname,NULL,0,
                                                        SCC_NO_NOTIFY|SCC_NO_DIRTIFY);
                sdsfree(cname);
                if (!consumer) {
                    rdbReportCorruptRDB("Duplicate stream consumer detected.");
                    decrRefCount(o);
                    return NULL;
                }

                consumer->seen_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                if (rioGetReadError(rdb)) {
                    rdbReportReadError("Stream short read reading seen time.");
                    decrRefCount(o);
                    return NULL;
                }

                if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_3) {
                    consumer->active_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                    if (rioGetReadError(rdb)) {
                        rdbReportReadError("Stream short read reading active time.");
                        decrRefCount(o);
                        return NULL;
                    }
                } else {
                    /* 这是我们能得到的最佳估计值 */
                    consumer->active_time = consumer->seen_time;
                }

                /* 加载此特定消费者所拥有条目的 PEL */
                pel_size = rdbLoadLen(rdb,NULL);
                if (pel_size == RDB_LENERR) {
                    rdbReportReadError(
                        "Stream consumer PEL num loading failed.");
                    decrRefCount(o);
                    return NULL;
                }
                while(pel_size--) {
                    unsigned char rawid[sizeof(streamID)];
                    if (rioRead(rdb,rawid,sizeof(rawid)) == 0) {
                        rdbReportReadError(
                            "Stream short read reading PEL streamID.");
                        decrRefCount(o);
                        return NULL;
                    }
                    void *result;
                    if (!raxFind(cgroup->pel,rawid,sizeof(rawid),&result)) {
                        rdbReportCorruptRDB("Consumer entry not found in "
                                                "group global PEL");
                        decrRefCount(o);
                        return NULL;
                    }
                    streamNACK *nack = result;

                    /* 设置 NACK 的消费者（在加载全局 PEL 时保留为 NULL）。
                     * 然后将同一个共享的 NACK 结构也设置到消费者特定的 PEL 中。 */
                    nack->consumer = consumer;
                    if (!raxTryInsert(consumer->pel,rawid,sizeof(rawid),nack,NULL)) {
                        rdbReportCorruptRDB("Duplicated consumer PEL entry "
                                                " loading a stream consumer "
                                                "group");
                        decrRefCount(o);
                        streamFreeNACK(nack);
                        return NULL;
                    }
                }
            }

            /* 验证每个 PEL 最终都有一个分配的消费者 */
            if (deep_integrity_validation) {
                raxIterator ri_cg_pel;
                raxStart(&ri_cg_pel,cgroup->pel);
                raxSeek(&ri_cg_pel,"^",NULL,0);
                while(raxNext(&ri_cg_pel)) {
                    streamNACK *nack = ri_cg_pel.data;
                    if (!nack->consumer) {
                        raxStop(&ri_cg_pel);
                        rdbReportCorruptRDB("Stream CG PEL entry without consumer");
                        decrRefCount(o);
                        return NULL;
                    }
                }
                raxStop(&ri_cg_pel);
            }
        }
    } else if (rdbtype == RDB_TYPE_MODULE_PRE_GA) {
            rdbReportCorruptRDB("Pre-release module format not supported");
            return NULL;
    } else if (rdbtype == RDB_TYPE_MODULE_2) {
        uint64_t moduleid = rdbLoadLen(rdb,NULL);
        if (rioGetReadError(rdb)) {
            rdbReportReadError("Short read module id");
            return NULL;
        }
        moduleType *mt = moduleTypeLookupModuleByID(moduleid);

        if (rdbCheckMode) {
            char name[10];
            moduleTypeNameByID(name,moduleid);
            return rdbLoadCheckModuleValue(rdb,name);
        }

        if (mt == NULL) {
            char name[10];
            moduleTypeNameByID(name,moduleid);
            rdbReportCorruptRDB("The RDB file contains module data I can't load: no matching module type '%s'", name);
            return NULL;
        }
        RedisModuleIO io;
        robj keyobj;
        initStaticStringObject(keyobj,key);
        moduleInitIOContext(io,mt,rdb,&keyobj,dbid);
        /* 调用模块的 rdb_load 方法，编码版本号
         * 存储在模块 ID 的低 10 位中。 */
        void *ptr = mt->rdb_load(&io,moduleid&1023);
        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }

        /* 模块 v2 序列化格式在末尾有一个 EOF 标记。 */
        uint64_t eof = rdbLoadLen(rdb,NULL);
        if (eof == RDB_LENERR) {
            if (ptr) {
                o = createModuleObject(mt, ptr); /* 创建对象以便轻松销毁 */
                decrRefCount(o);
            }
            return NULL;
        }
        if (eof != RDB_MODULE_OPCODE_EOF) {
            rdbReportCorruptRDB("The RDB file contains module data for the module '%s' that is not terminated by "
                                "the proper module value EOF marker", moduleTypeModuleName(mt));
            if (ptr) {
                o = createModuleObject(mt, ptr); /* 创建对象以便轻松销毁 */
                decrRefCount(o);
            }
            return NULL;
        }

        if (ptr == NULL) {
            rdbReportCorruptRDB("The RDB file contains module data for the module type '%s', that the responsible "
                                "module is not able to load. Check for modules log above for additional clues.",
                                moduleTypeModuleName(mt));
            return NULL;
        }
        o = createModuleObject(mt, ptr);
    } else {
        rdbReportReadError("Unknown RDB encoding type %d",rdbtype);
        return NULL;
    }

    if (error) *error = 0;
    return o;

emptykey:
    if (error) *error = RDB_LOAD_ERR_EMPTY_KEY;
    return NULL;
}

/* 标记服务器正在加载状态，并初始化用于显示加载进度的字段。 */
void startLoading(size_t size, int rdbflags, int async) {
    /* 加载数据库 */
    server.loading = 1;
    if (async == 1) server.async_loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    server.loading_total_bytes = size;
    server.loading_rdb_used_mem = 0;
    server.rdb_last_load_keys_expired = 0;
    server.rdb_last_load_keys_loaded = 0;
    blockingOperationStarts();

    /* 触发加载模块开始事件 */
    int subevent;
    if (rdbflags & RDBFLAGS_AOF_PREAMBLE)
        subevent = REDISMODULE_SUBEVENT_LOADING_AOF_START;
    else if(rdbflags & RDBFLAGS_REPLICATION)
        subevent = REDISMODULE_SUBEVENT_LOADING_REPL_START;
    else
        subevent = REDISMODULE_SUBEVENT_LOADING_RDB_START;
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,subevent,NULL);
}

/* 标记服务器正在加载状态，并初始化用于显示加载进度的字段。
 * 'filename' 为可选参数，用于出错时的 rdb 检查。 */
void startLoadingFile(size_t size, char* filename, int rdbflags) {
    rdbFileBeingLoaded = filename;
    startLoading(size, rdbflags, 0);
}

/* 更新绝对加载进度信息 */
void loadingAbsProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* 更新增量加载进度信息 */
void loadingIncrProgress(off_t size) {
    server.loading_loaded_bytes += size;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* 更新当前正在加载的文件名 */
void updateLoadingFileName(char* filename) {
    rdbFileBeingLoaded = filename;
}

/* 加载完成 */
void stopLoading(int success) {
    server.loading = 0;
    server.async_loading = 0;
    blockingOperationEnds();
    rdbFileBeingLoaded = NULL;

    /* 触发加载模块结束事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,
                          success?
                            REDISMODULE_SUBEVENT_LOADING_ENDED:
                            REDISMODULE_SUBEVENT_LOADING_FAILED,
                           NULL);
}

/* 通知模块持久化操作开始。根据标志和进程类型
 * 选择不同的子事件类型。 */
void startSaving(int rdbflags) {
    /* 触发持久化模块开始事件 */
    int subevent;
    if (rdbflags & RDBFLAGS_AOF_PREAMBLE && getpid() != server.pid)
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_AOF_START;
    else if (rdbflags & RDBFLAGS_AOF_PREAMBLE)
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_AOF_START;
    else if (getpid()!=server.pid)
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START;
    else
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START;
    moduleFireServerEvent(REDISMODULE_EVENT_PERSISTENCE,subevent,NULL);
}

/* 通知模块持久化操作结束。 */
void stopSaving(int success) {
    /* 触发持久化模块结束事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_PERSISTENCE,
                          success?
                            REDISMODULE_SUBEVENT_PERSISTENCE_ENDED:
                            REDISMODULE_SUBEVENT_PERSISTENCE_FAILED,
                          NULL);
}

/* 跟踪加载进度，以便在加载过程中定期服务客户端请求，
 * 并在需要时计算 RDB 校验和。 */
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER)
            replicationSendNewlineToMaster();
        loadingAbsProgress(r->processed_bytes);
        processEventsWhileBlocked();
        processModuleLoadingProgressEvent(0);
    }
    if (server.repl_state == REPL_STATE_TRANSFER && rioCheckType(r) == RIO_TYPE_CONN) {
        atomicIncr(server.stat_net_repl_input_bytes, len);
    }
}

/* 从 RDB 中加载函数库。
 * err 输出参数为可选，失败时会被设置为相关错误信息，
 * 调用者负责在失败时释放该错误信息。
 *
 * lib_ctx 参数也是可选的。如果传入 NULL，
 * 则仅验证 RDB 结构而不执行实际的函数加载。 */
int rdbFunctionLoad(rio *rdb, int ver, functionsLibCtx* lib_ctx, int rdbflags, sds *err) {
    UNUSED(ver);
    sds error = NULL;
    sds final_payload = NULL;
    int res = C_ERR;
    if (!(final_payload = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))) {
        error = sdsnew("Failed loading library payload");
        goto done;
    }

    if (lib_ctx) {
        sds library_name = NULL;
        if (!(library_name = functionsCreateWithLibraryCtx(final_payload, rdbflags & RDBFLAGS_ALLOW_DUP, &error, lib_ctx, 0))) {
            if (!error) {
                error = sdsnew("Failed creating the library");
            }
            goto done;
        }
        sdsfree(library_name);
    }

    res = C_OK;

done:
    if (final_payload) sdsfree(final_payload);
    if (error) {
        if (err) {
            *err = error;
        } else {
            serverLog(LL_WARNING, "Failed creating function, %s", error);
            sdsfree(error);
        }
    }
    return res;
}

/* 从 rio 流 'rdb' 中加载 RDB 文件。
 * 成功返回 C_OK，否则返回 C_ERR 并设置 errno。 */
int rdbLoadRio(rio *rdb, int rdbflags, rdbSaveInfo *rsi) {
    functionsLibCtx* functions_lib_ctx = functionsLibCtxGetCurrent();
    rdbLoadingCtx loading_ctx = { .dbarray = server.db, .functions_lib_ctx = functions_lib_ctx };
    int retval = rdbLoadRioWithLoadingCtx(rdb,rdbflags,rsi,&loading_ctx);
    return retval;
}

/* 从 rio 流 'rdb' 中加载 RDB 文件。成功返回 C_OK，否则返回 C_ERR。
 * rdb_loading_ctx 参数保存 RDB 数据加载的目标对象，
 * 当前仅允许设置数据库对象和 functionsLibCtx，
 * 未来可能会包含更多此类对象。 */
int rdbLoadRioWithLoadingCtx(rio *rdb, int rdbflags, rdbSaveInfo *rsi, rdbLoadingCtx *rdb_loading_ctx) {
    uint64_t dbid = 0;
    int type, rdbver;
    uint64_t db_size = 0, expires_size = 0;
    int should_expand_db = 0;
    redisDb *db = rdb_loading_ctx->dbarray+0;
    char buf[1024];
    int error;
    long long empty_keys_skipped = 0;

    rdb->update_cksum = rdbLoadProgressCallback;
    rdb->max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        serverLog(LL_WARNING,"Wrong signature trying to load DB from file");
        return C_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        serverLog(LL_WARNING,"Can't handle RDB format version %d",rdbver);
        return C_ERR;
    }

    /* 键相关属性，由键类型之前的特殊操作码设置 */
    long long lru_idle = -1, lfu_freq = -1, expiretime = -1, now = mstime();
    long long lru_clock = LRU_CLOCK();

    while(1) {
        sds key;
        robj *val;

        /* 读取类型 */
        if ((type = rdbLoadType(rdb)) == -1) goto eoferr;

        /* 处理特殊类型 */
        if (type == RDB_OPCODE_EXPIRETIME) {
            /* EXPIRETIME: 加载与下一个键关联的过期时间。
             * 注意：加载过期时间后需要继续读取实际类型。 */
            expiretime = rdbLoadTime(rdb);
            expiretime *= 1000;
            if (rioGetReadError(rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: 毫秒精度的过期时间，
             * 从 RDB v3 引入。与 EXPIRETIME 类似但精度更高。 */
            expiretime = rdbLoadMillisecondTime(rdb,rdbver);
            if (rioGetReadError(rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_FREQ) {
            /* FREQ: LFU 访问频率 */
            uint8_t byte;
            if (rioRead(rdb,&byte,1) == 0) goto eoferr;
            lfu_freq = byte;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_IDLE) {
            /* IDLE: LRU 空闲时间 */
            uint64_t qword;
            if ((qword = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto eoferr;
            lru_idle = qword;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: 文件结束，退出主循环 */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: 选择指定的数据库 */
            if ((dbid = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto eoferr;
            if (dbid >= (unsigned)server.dbnum) {
                serverLog(LL_WARNING,
                    "FATAL: Data file was created with a Redis "
                    "server configured to handle more than %d "
                    "databases. Exiting\n", server.dbnum);
                exit(1);
            }
            db = rdb_loading_ctx->dbarray+dbid;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: 提示当前选中数据库的键数量，
             * 以避免不必要的 rehash。 */
            if ((db_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            should_expand_db = 1;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_SLOT_INFO) {
            uint64_t slot_id, slot_size, expires_slot_size;
            if ((slot_id = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((slot_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_slot_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if (!server.cluster_enabled) {
                continue; /* Ignore gracefully. */
            }
            /* 在集群模式下，根据各 slot 的键数量对
             * 对应的字典进行扩容。 */
            kvstoreDictExpand(db->keys, slot_id, slot_size);
            kvstoreDictExpand(db->expires, slot_id, expires_slot_size);
            should_expand_db = 0;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: 通用的字符串键值对字段。用于向 RDB 添加
             * 向后兼容的状态信息。RDB 加载实现必须跳过
             * 无法识别的 AUX 字段。
             *
             * AUX 字段由两个字符串组成：键和值。 */
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(rdb)) == NULL) {
                decrRefCount(auxkey);
                goto eoferr;
            }

            if (((char*)auxkey->ptr)[0] == '%') {
                /* 所有以 '%' 开头的字段名被视为信息字段，
                 * 启动时以 NOTICE 级别记录日志。 */
                serverLog(LL_NOTICE,"RDB '%s': %s",
                    (char*)auxkey->ptr,
                    (char*)auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr,"repl-stream-db")) {
                if (rsi) rsi->repl_stream_db = atoi(auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr,"repl-id")) {
                if (rsi && sdslen(auxval->ptr) == CONFIG_RUN_ID_SIZE) {
                    memcpy(rsi->repl_id,auxval->ptr,CONFIG_RUN_ID_SIZE+1);
                    rsi->repl_id_is_set = 1;
                }
            } else if (!strcasecmp(auxkey->ptr,"repl-offset")) {
                if (rsi) rsi->repl_offset = strtoll(auxval->ptr,NULL,10);
            } else if (!strcasecmp(auxkey->ptr,"lua")) {
                /* 不再将脚本加载回内存 */
            } else if (!strcasecmp(auxkey->ptr,"redis-ver")) {
                serverLog(LL_NOTICE,"Loading RDB produced by version %s",
                    (char*)auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr,"ctime")) {
                time_t age = time(NULL)-strtol(auxval->ptr,NULL,10);
                if (age < 0) age = 0;
                serverLog(LL_NOTICE,"RDB age %ld seconds",
                    (unsigned long) age);
            } else if (!strcasecmp(auxkey->ptr,"used-mem")) {
                long long usedmem = strtoll(auxval->ptr,NULL,10);
                serverLog(LL_NOTICE,"RDB memory usage when created %.2f Mb",
                    (double) usedmem / (1024*1024));
                server.loading_rdb_used_mem = usedmem;
            } else if (!strcasecmp(auxkey->ptr,"aof-preamble")) {
                long long haspreamble = strtoll(auxval->ptr,NULL,10);
                if (haspreamble) serverLog(LL_NOTICE,"RDB has an AOF tail");
            } else if (!strcasecmp(auxkey->ptr, "aof-base")) {
                long long isbase = strtoll(auxval->ptr, NULL, 10);
                if (isbase) serverLog(LL_NOTICE, "RDB is base AOF");
            } else if (!strcasecmp(auxkey->ptr,"redis-bits")) {
                /* 忽略该字段 */
            } else {
                /* 按照 AUX 字段约定，忽略无法识别的字段 */
                serverLog(LL_DEBUG,"Unrecognized RDB AUX field: '%s'",
                    (char*)auxkey->ptr);
            }

            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_MODULE_AUX) {
            /* 加载与 Redis 键空间无关的模块数据。
             * 此类数据可能存储在 RDB 键值部分之前或之后。 */
            uint64_t moduleid = rdbLoadLen(rdb,NULL);
            int when_opcode = rdbLoadLen(rdb,NULL);
            int when = rdbLoadLen(rdb,NULL);
            if (rioGetReadError(rdb)) goto eoferr;
            if (when_opcode != RDB_MODULE_OPCODE_UINT) {
                rdbReportReadError("bad when_opcode");
                goto eoferr;
            }
            moduleType *mt = moduleTypeLookupModuleByID(moduleid);
            char name[10];
            moduleTypeNameByID(name,moduleid);

            if (!rdbCheckMode && mt == NULL) {
                /* 未知模块 */
                serverLog(LL_WARNING,"The RDB file contains AUX module data I can't load: no matching module '%s'", name);
                exit(1);
            } else if (!rdbCheckMode && mt != NULL) {
                if (!mt->aux_load) {
                    /* 模块不支持 AUX */
                    serverLog(LL_WARNING,"The RDB file contains module AUX data, but the module '%s' doesn't seem to support it.", name);
                    exit(1);
                }

                RedisModuleIO io;
                moduleInitIOContext(io,mt,rdb,NULL,-1);
                /* 调用模块的 rdb_load 方法，编码版本号
                 * 存储在模块 ID 的低 10 位中。 */
                int rc = mt->aux_load(&io,moduleid&1023, when);
                if (io.ctx) {
                    moduleFreeContext(io.ctx);
                    zfree(io.ctx);
                }
                if (rc != REDISMODULE_OK || io.error) {
                    moduleTypeNameByID(name,moduleid);
                    serverLog(LL_WARNING,"The RDB file contains module AUX data for the module type '%s', that the responsible module is not able to load. Check for modules log above for additional clues.", name);
                    goto eoferr;
                }
                uint64_t eof = rdbLoadLen(rdb,NULL);
                if (eof != RDB_MODULE_OPCODE_EOF) {
                    serverLog(LL_WARNING,"The RDB file contains module AUX data for the module '%s' that is not terminated by the proper module value EOF marker", name);
                    goto eoferr;
                }
                continue;
            } else {
                /* RDB 检查模式 */
                robj *aux = rdbLoadCheckModuleValue(rdb,name);
                decrRefCount(aux);
                continue; /* Read next opcode. */
            }
        } else if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
            rdbReportCorruptRDB("Pre-release function format not supported.");
            exit(1);
        } else if (type == RDB_OPCODE_FUNCTION2) {
            sds err = NULL;
            if (rdbFunctionLoad(rdb, rdbver, rdb_loading_ctx->functions_lib_ctx, rdbflags, &err) != C_OK) {
                serverLog(LL_WARNING,"Failed loading library, %s", err);
                sdsfree(err);
                goto eoferr;
            }
            continue;
        }

        /* 如果没有 slot 信息，说明不在集群模式或正在加载
         * 旧版 RDB 文件。此时需要估算每个 slot 的键数量并相应扩容。 */
        if (should_expand_db) {
            dbExpand(db, db_size, 0);
            dbExpandExpires(db, expires_size, 0);
            should_expand_db = 0;
        }

        /* 读取键 */
        if ((key = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL)
            goto eoferr;
        /* 读取值 */
        val = rdbLoadObject(type,rdb,key,db->id,&error);

        /* 检查键是否已过期。此函数用于从磁盘加载 RDB 文件
         * 时，无论是在启动时还是从主节点接收 RDB 时。
         * 在后一种情况下，主节点负责键的过期。如果在此处
         * 过期键，主节点的快照可能无法反映到从节点。
         * 同理，如果基础 AOF 是 RDB 格式，我们需要加载所有键，
         * 因为增量 AOF 中的操作日志假定在精确的键空间状态下运行。 */
        if (val == NULL) {
            /* 由于我们曾经遇到可能导致空键的 bug（参见 #8453），
             * 遇到 RDB 文件中的空键时不报错，
             * 而是静默丢弃并继续加载。 */
            if (error == RDB_LOAD_ERR_EMPTY_KEY) {
                if(empty_keys_skipped++ < 10)
                    serverLog(LL_NOTICE, "rdbLoadObject skipping empty key: %s", key);
                sdsfree(key);
            } else {
                sdsfree(key);
                goto eoferr;
            }
        } else if (iAmMaster() &&
            !(rdbflags&RDBFLAGS_AOF_PREAMBLE) &&
            expiretime != -1 && expiretime < now)
        {
            if (rdbflags & RDBFLAGS_FEED_REPL) {
                /* 调用者应已创建复制积压缓冲区，
                 * 此路径仅在重启时生效，
                 * 因此此时没有从节点。 */
                serverAssert(server.repl_backlog != NULL && listLength(server.slaves) == 0);
                robj keyobj;
                initStaticStringObject(keyobj,key);
                robj *argv[2];
                argv[0] = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
                argv[1] = &keyobj;
                replicationFeedSlaves(server.slaves,dbid,argv,2);
            }
            sdsfree(key);
            decrRefCount(val);
            server.rdb_last_load_keys_expired++;
        } else {
            robj keyobj;
            initStaticStringObject(keyobj,key);

            /* 将新对象添加到哈希表 */
            int added = dbAddRDBLoad(db,key,val);
            server.rdb_last_load_keys_loaded++;
            if (!added) {
                if (rdbflags & RDBFLAGS_ALLOW_DUP) {
                    /* 此标志用于 DEBUG RELOAD 特殊模式。
                     * 设置后允许新键替换同名的现有键。 */
                    dbSyncDelete(db,&keyobj);
                    dbAddRDBLoad(db,key,val);
                } else {
                    serverLog(LL_WARNING,
                        "RDB has duplicated key '%s' in DB %d",key,db->id);
                    serverPanic("Duplicated key found in RDB file");
                }
            }

            /* 如果设置了 minExpiredField，则对象是带字段过期的哈希，
             * 需要在全局 HFE DS 中注册 */
            if (val->type == OBJ_HASH) {
                uint64_t minExpiredField = hashTypeGetMinExpire(val, 1);
                if (minExpiredField != EB_EXPIRE_TIME_INVALID)
                    hashTypeAddToExpires(db, key, val, minExpiredField);
            }

            /* 根据需要设置过期时间 */
            if (expiretime != -1) {
                setExpire(NULL,db,&keyobj,expiretime);
            }

            /* 设置使用信息（用于淘汰策略） */
            objectSetLRUOrLFU(val,lfu_freq,lru_idle,lru_clock,1000);

            /* 仅为模块触发键空间加载通知 */
            moduleNotifyKeyspaceEvent(NOTIFY_LOADED, "loaded", &keyobj, db->id);
        }

        /* 以较慢的速度加载数据库，用于测试某些边缘情况。 */
        if (server.key_load_delay)
            debugDelay(server.key_load_delay);

        /* 重置由之前操作码设置的键相关状态，
         * 以便从头开始处理下一个键。 */
        expiretime = -1;
        lfu_freq = -1;
        lru_idle = -1;
    }
    /* 如果 RDB 版本 >= 5，验证校验和 */
    if (rdbver >= 5) {
        uint64_t cksum, expected = rdb->cksum;

        if (rioRead(rdb,&cksum,8) == 0) goto eoferr;
        if (server.rdb_checksum && !server.skip_checksum_validation) {
            memrev64ifbe(&cksum);
            if (cksum == 0) {
                serverLog(LL_NOTICE,"RDB file was saved with checksum disabled: no check performed.");
            } else if (cksum != expected) {
                serverLog(LL_WARNING,"Wrong RDB checksum expected: (%llx) but "
                    "got (%llx). Aborting now.",
                        (unsigned long long)expected,
                        (unsigned long long)cksum);
                rdbReportCorruptRDB("RDB CRC error");
                return C_ERR;
            }
        }
    }

    if (empty_keys_skipped) {
        serverLog(LL_NOTICE,
            "Done loading RDB, keys loaded: %lld, keys expired: %lld, empty keys skipped: %lld.",
                server.rdb_last_load_keys_loaded, server.rdb_last_load_keys_expired, empty_keys_skipped);
    } else {
        serverLog(LL_NOTICE,
            "Done loading RDB, keys loaded: %lld, keys expired: %lld.",
                server.rdb_last_load_keys_loaded, server.rdb_last_load_keys_expired);
    }
    return C_OK;

    /* 意外的文件结尾通过调用 rdbReportReadError() 处理：
     * 大多数情况下会终止 Redis，但如果我们在初始 SYNC 期间
     * 从套接字加载 RDB 文件（无盘副本模式），
     * 则将错误报告给调用者以便重试。 */
eoferr:
    serverLog(LL_WARNING,
        "Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbReportReadError("Unexpected EOF reading RDB file");
    return C_ERR;
}

/* 类似于 rdbLoadRio()，但接受文件名而非 rio 流。
 * 该函数打开文件进行读取并创建 rio 流对象来执行加载。
 * 同时初始化和完成 INFO 输出中显示的 ETA。
 *
 * 如果传入使用 RDB_SAVE_INFO_INIT 初始化的 'rsi' 结构，
 * 加载代码将填充该结构中的信息字段。 */
int rdbLoad(char *filename, rdbSaveInfo *rsi, int rdbflags) {
    FILE *fp;
    rio rdb;
    int retval;
    struct stat sb;
    int rdb_fd;

    fp = fopen(filename, "r");
    if (fp == NULL) {
        if (errno == ENOENT) return RDB_NOT_EXIST;

        serverLog(LL_WARNING,"Fatal error: can't open the RDB file %s for reading: %s", filename, strerror(errno));
        return RDB_FAILED;
    }

    if (fstat(fileno(fp), &sb) == -1)
        sb.st_size = 0;

    startLoadingFile(sb.st_size, filename, rdbflags);
    rioInitWithFile(&rdb,fp);

    retval = rdbLoadRio(&rdb,rdbflags,rsi);

    fclose(fp);
    stopLoading(retval==C_OK);
    /* 回收 RDB 文件占用的页缓存 */
    if (retval == C_OK && !(rdbflags & RDBFLAGS_KEEP_CACHE)) {
        /* TODO: 未来或许可以将 fopen 和 open 合并为一次操作 */
        rdb_fd = open(filename, O_RDONLY);
        if (rdb_fd >= 0) bioCreateCloseJob(rdb_fd, 0, 1);
    }
    return (retval==C_OK) ? RDB_OK : RDB_FAILED;
}

/* 后台保存子进程（BGSAVE）完成后的处理函数。
 * 此函数处理实际的 BGSAVE 磁盘写入场景。 */
static void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal, time_t save_end) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = save_end;
        server.lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        server.lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;

        serverLog(LL_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(server.child_pid, 0);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        /* SIGUSR1 被加入白名单，因此我们可以用它
         * 终止子进程而不触发错误状态。 */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = C_ERR;
    }
}

/* 后台保存子进程（BGSAVE）完成后的处理函数。
 * 此函数处理无盘复制中 RDB -> 从节点套接字传输的场景。 */
static void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background RDB transfer terminated with success");
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background transfer error");
    } else {
        serverLog(LL_WARNING,
            "Background transfer terminated by signal %d", bysignal);
    }
    if (server.rdb_child_exit_pipe!=-1)
        close(server.rdb_child_exit_pipe);
    aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
    close(server.rdb_pipe_read);
    server.rdb_child_exit_pipe = -1;
    server.rdb_pipe_read = -1;
    zfree(server.rdb_pipe_conns);
    server.rdb_pipe_conns = NULL;
    server.rdb_pipe_numconns = 0;
    server.rdb_pipe_numconns_writing = 0;
    zfree(server.rdb_pipe_buff);
    server.rdb_pipe_buff = NULL;
    server.rdb_pipe_bufflen = 0;
}

/* 当后台 RDB 保存/传输完成时，调用相应的处理函数。 */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    int type = server.rdb_child_type;
    time_t save_end = time(NULL);
    if (server.bgsave_aborted)
        bysignal = SIGUSR1;
    switch(server.rdb_child_type) {
    case RDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal,save_end);
        break;
    case RDB_CHILD_TYPE_SOCKET:
        backgroundSaveDoneHandlerSocket(exitcode,bysignal);
        break;
    default:
        serverPanic("Unknown RDB child type.");
        break;
    }

    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = save_end-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    server.bgsave_aborted = 0;
    /* 可能有从节点正在等待 BGSAVE 以完成同步
     * （SYNC 的第一阶段是 dump.rdb 的批量传输） */
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, type);
}

/* 使用 SIGUSR1 终止 RDB 保存子进程（使父进程知道
 * 子进程并非因错误退出，而是被主动终止），
 * 并执行必要的清理工作。 */
void killRDBChild(void) {
    kill(server.child_pid, SIGUSR1);
    /* 由于此处未使用 waitpid（与 killAppendOnlyChild
     * 和 TerminateModuleForkChild 不同），所有清理操作
     * 由 checkChildrenDone 完成，它稍后会发现进程已被终止。
     * 清理操作包括：
     * - resetChildState
     * - rdbRemoveTempFile */

    /* 然而，子进程可能已经退出（或即将退出），
     * 因此无法接收信号。在这种情况下子进程可能返回成功，
     * 完成处理函数会错误地覆盖某些服务器指标
     * （如 dirty 计数器，例如在 FLUSHALL 时），
     * 或覆盖同步创建的 RDB 文件。 */
     server.bgsave_aborted = 1;
}

/* 创建一个 RDB 子进程，将 RDB 写入处于
 * SLAVE_STATE_WAIT_BGSAVE_START 状态的从节点套接字。 */
int rdbSaveToSlavesSockets(int req, rdbSaveInfo *rsi) {
    listNode *ln;
    listIter li;
    pid_t childpid;
    int pipefds[2], rdb_pipe_write, safe_to_exit_pipe;

    if (hasActiveChildProcess()) return C_ERR;

    /* 即使上一个 fork 子进程已退出，在管道数据
     * 未完全读取前不要启动新的子进程。 */
    if (server.rdb_pipe_conns) return C_ERR;

    /* 在 fork 之前创建一个管道，用于将 RDB 字节传输到父进程。
     * 不能让子进程直接写入套接字，因为在 TLS 情况下
     * 子进程终止后父进程接管时必须保持连续的 TLS 状态。 */
    if (anetPipe(pipefds, O_NONBLOCK, 0) == -1) return C_ERR;
    server.rdb_pipe_read = pipefds[0]; /* read end */
    rdb_pipe_write = pipefds[1]; /* write end */

    /* 创建另一个管道，用于父进程通知子进程可以退出。 */
    if (anetPipe(pipefds, 0, 0) == -1) {
        close(rdb_pipe_write);
        close(server.rdb_pipe_read);
        return C_ERR;
    }
    safe_to_exit_pipe = pipefds[0]; /* read end */
    server.rdb_child_exit_pipe = pipefds[1]; /* write end */

    /* 收集需要传输 RDB 的从节点连接，
     * 即处于 WAIT_BGSAVE_START 状态的从节点。 */
    server.rdb_pipe_conns = zmalloc(sizeof(connection *)*listLength(server.slaves));
    server.rdb_pipe_numconns = 0;
    server.rdb_pipe_numconns_writing = 0;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
            /* 检查从节点是否有精确匹配的要求 */
            if (slave->slave_req != req)
                continue;
            server.rdb_pipe_conns[server.rdb_pipe_numconns++] = slave->conn;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
        }
    }

    /* 创建子进程 */
    if ((childpid = redisFork(CHILD_TYPE_RDB)) == 0) {
        /* Child */
        int retval, dummy;
        rio rdb;

        rioInitWithFd(&rdb,rdb_pipe_write);

        /* 关闭读取端，这样如果父进程崩溃，
         * 子进程会收到写入错误并退出。 */
        close(server.rdb_pipe_read);

        redisSetProcTitle("redis-rdb-to-slaves");
        redisSetCpuAffinity(server.bgsave_cpulist);

        retval = rdbSaveRioWithEOFMark(req,&rdb,NULL,rsi);
        if (retval == C_OK && rioFlush(&rdb) == 0)
            retval = C_ERR;

        if (retval == C_OK) {
            sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
        }

        rioFreeFd(&rdb);
        /* 唤醒读取端，通知已完成 */
        close(rdb_pipe_write);
        close(server.rdb_child_exit_pipe); /* 关闭写入端以便父进程检测到关闭 */
        /* 等待父进程通知可以安全退出。我们不期望读取到
         * 任何数据，仅在管道关闭时收到错误。 */
        dummy = read(safe_to_exit_pipe, pipefds, 1);
        UNUSED(dummy);
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));

            /* 撤销状态变更。调用者将对所有处于
             * BGSAVE_START 状态的从节点执行清理，
             * 但 replicationSetupSlaveForFullResync() 的提前调用
             * 已将其转为 BGSAVE_END */
            listRewind(server.slaves,&li);
            while((ln = listNext(&li))) {
                client *slave = ln->value;
                if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
                    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
                }
            }
            close(rdb_pipe_write);
            close(server.rdb_pipe_read);
            close(server.rdb_child_exit_pipe);
            zfree(server.rdb_pipe_conns);
            server.rdb_pipe_conns = NULL;
            server.rdb_pipe_numconns = 0;
            server.rdb_pipe_numconns_writing = 0;
        } else {
            serverLog(LL_NOTICE,"Background RDB transfer started by pid %ld",
                (long) childpid);
            server.rdb_save_time_start = time(NULL);
            server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;
            close(rdb_pipe_write); /* 在父进程中关闭写入端以便检测子进程关闭 */
            if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
                serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
            }
        }
        close(safe_to_exit_pipe);
        return (childpid == -1) ? C_ERR : C_OK;
    }
    return C_OK; /* 不可达 */
}

/* SAVE 命令实现。同步保存数据库到磁盘。
 * 如果后台保存已在进行中则返回错误。 */
void saveCommand(client *c) {
    if (server.child_type == CHILD_TYPE_RDB) {
        addReplyError(c,"Background save already in progress");
        return;
    }

    server.stat_rdb_saves++;

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    if (rdbSave(SLAVE_REQ_NONE,server.rdb_filename,rsiptr,RDBFLAGS_NONE) == C_OK) {
        addReply(c,shared.ok);
    } else {
        addReplyErrorObject(c,shared.err);
    }
}

/* BGSAVE [SCHEDULE] 命令实现 */
void bgsaveCommand(client *c) {
    int schedule = 0;

    /* SCHEDULE 选项改变了 BGSAVE 在 AOF 重写进行中的行为。
     * 不返回错误，而是调度一个 BGSAVE。 */
    if (c->argc > 1) {
        if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"schedule")) {
            schedule = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);

    if (server.child_type == CHILD_TYPE_RDB) {
        addReplyError(c,"Background save already in progress");
    } else if (hasActiveChildProcess() || server.in_exec) {
        if (schedule || server.in_exec) {
            server.rdb_bgsave_scheduled = 1;
            addReplyStatus(c,"Background saving scheduled");
        } else {
            addReplyError(c,
            "Another child process is active (AOF?): can't BGSAVE right now. "
            "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever "
            "possible.");
        }
    } else if (rdbSaveBackground(SLAVE_REQ_NONE,server.rdb_filename,rsiptr,RDBFLAGS_NONE) == C_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReplyErrorObject(c,shared.err);
    }
}

/* 填充用于在 RDB 文件中持久化复制信息的 rdbSaveInfo 结构。
 * 当前该结构仅显式包含主节点流中当前选中的数据库。
 * 然而如果 rdbSave*() 系列函数收到 NULL rsi 结构，
 * 复制 ID/偏移量也不会被保存。
 * 函数填充 'rsi'（通常由调用者在栈上分配），
 * 如果实例有有效的主客户端则返回填充后的指针，
 * 否则返回 NULL，此时 RDB 保存不会持久化任何复制相关信息。 */
rdbSaveInfo *rdbPopulateSaveInfo(rdbSaveInfo *rsi) {
    rdbSaveInfo rsi_init = RDB_SAVE_INFO_INIT;
    *rsi = rsi_init;

    /* 如果实例是主节点，只有在 repl_backlog 不为 NULL 时
     * 才能填充复制信息。如果 repl_backlog 为 NULL，
     * 说明实例不在任何复制链中。在这种场景下复制信息无用，
     * 因为当从节点连接到我们时，NULL 的 repl_backlog 会触发
     * 全量同步，同时我们会使用新的 replid 并清空旧的。 */
    if (!server.masterhost && server.repl_backlog) {
        /* Note that when server.slaveseldb is -1, it means that this master
         * didn't apply any write commands after a full synchronization.
         * So we can let repl_stream_db be 0, this allows a restarted slave
         * to reload replication ID/offset, it's safe because the next write
         * command must generate a SELECT statement. */
        rsi->repl_stream_db = server.slaveseldb == -1 ? 0 : server.slaveseldb;
        return rsi;
    }

    /* If the instance is a slave we need a connected master
     * in order to fetch the currently selected DB. */
    if (server.master) {
        rsi->repl_stream_db = server.master->db->id;
        return rsi;
    }

    /* If we have a cached master we can use it in order to populate the
     * replication selected DB info inside the RDB file: the slave can
     * increment the master_repl_offset only from data arriving from the
     * master, so if we are disconnected the offset in the cached master
     * is valid. */
    if (server.cached_master) {
        rsi->repl_stream_db = server.cached_master->db->id;
        return rsi;
    }
    return NULL;
}
