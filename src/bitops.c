/* 位运算操作。
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * 保留所有权利。
 *
 * 根据 Redis 源码可用许可证 2.0 (RSALv2)
 * 或服务器端公共许可证 v1 (SSPLv1) 授权使用。
 */

#include "server.h"

/* -----------------------------------------------------------------------------
 * 辅助函数和底层位运算函数。
 * -------------------------------------------------------------------------- */

/* 统计指针 's' 指向的二进制数组中前 'count' 个字节里被置 1 的位的数量。
 * 该函数的实现必须能处理长度达到 512 MB 甚至更大的输入字符串
 * （即 server.proto_max_bulk_len）。
 * 时间复杂度：O(N/28)，其中 N 是字节数 */
long long redisPopcount(void *s, long count) {
    long long bits = 0;
    unsigned char *p = s;
    uint32_t *p4;
    /* 0~255 每个字节值中 1 的位数查找表，用于快速按字节统计 */
    static const unsigned char bitsinbyte[256] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

    /* 先统计开头未按 32 位对齐的字节，逐字节累加 */
    while((unsigned long)p & 3 && count) {
        bits += bitsinbyte[*p++];
        count--;
    }

    /* 每次按 28 字节进行位计数（并行位操作） */
    p4 = (uint32_t*)p;
    while(count>=28) {
        uint32_t aux1, aux2, aux3, aux4, aux5, aux6, aux7;

        aux1 = *p4++;
        aux2 = *p4++;
        aux3 = *p4++;
        aux4 = *p4++;
        aux5 = *p4++;
        aux6 = *p4++;
        aux7 = *p4++;
        count -= 28;

        /* 经典的位计数（popcount）算法：先两两分组、再四四分组，最后求和 */
        aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
        aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
        aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
        aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
        aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
        aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
        aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
        aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
        aux5 = aux5 - ((aux5 >> 1) & 0x55555555);
        aux5 = (aux5 & 0x33333333) + ((aux5 >> 2) & 0x33333333);
        aux6 = aux6 - ((aux6 >> 1) & 0x55555555);
        aux6 = (aux6 & 0x33333333) + ((aux6 >> 2) & 0x33333333);
        aux7 = aux7 - ((aux7 >> 1) & 0x55555555);
        aux7 = (aux7 & 0x33333333) + ((aux7 >> 2) & 0x33333333);
        bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) +
                    ((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) +
                    ((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) +
                    ((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) +
                    ((aux5 + (aux5 >> 4)) & 0x0F0F0F0F) +
                    ((aux6 + (aux6 >> 4)) & 0x0F0F0F0F) +
                    ((aux7 + (aux7 >> 4)) & 0x0F0F0F0F))* 0x01010101) >> 24;
    }
    /* 对剩余不足 28 字节的部分，继续使用查表法逐字节统计 */
    p = (unsigned char*)p4;
    while(count--) bits += bitsinbyte[*p++];
    return bits;
}

/* 返回位图中第一个等于指定值（'bit' 为 1 时查找 1，为 0 时查找 0）的位的位置。
 * 位图从 's' 开始，长度为 'count' 字节。
 *
 * 当 'bit' 为 0 时，函数保证返回值 >= 0：如果未找到 0 位，
 * 则假设字符串右侧被零填充，返回 count*8。
 * 但当 'bit' 为 1 时，位图中可能没有任何一个置 1 的位，
 * 这种特殊情况下返回 -1。
 * 时间复杂度：O(N) 最坏情况，但通常更快（按字长跳跃） */
long long redisBitpos(void *s, unsigned long count, int bit) {
    unsigned long *l;
    unsigned char *c;
    unsigned long skipval, word = 0, one;
    long long pos = 0; /* 要返回给调用方的位位置 */
    unsigned long j;
    int found;

    /* 首先按整字处理：寻找第一个不全为 0 或不全为 1 的字，
     * 这取决于我们是在查找 0 还是 1。
     * 对于含有连续 0 或 1 比特块的大字符串，
     * 这种方式比逐位朴素处理要快得多。
     *
     * 注意：如果起始地址没有按 sizeof(unsigned long) 对齐，
     * 我们会先逐字节处理，直到对齐为止。 */

    /* 跳过起始处未按 sizeof(unsigned long) 对齐的位（逐字节处理） */
    skipval = bit ? 0 : UCHAR_MAX;
    c = (unsigned char*) s;
    found = 0;
    while((unsigned long)c & (sizeof(*l)-1) && count) {
        if (*c != skipval) {
            found = 1;
            break;
        }
        c++;
        count--;
        pos += 8;
    }

    /* 按完整字长跳过无关位 */
    l = (unsigned long*) c;
    if (!found) {
        skipval = bit ? 0 : ULONG_MAX;
        while (count >= sizeof(*l)) {
            if (*l != skipval) break;
            l++;
            count -= sizeof(*l);
            pos += sizeof(*l)*8;
        }
    }

    /* 把字节加载到 "word" 中，将第一个字节视为最高有效字节
     * （即按大端序处理，因为我们把字符串视为自左向右的比特序列，
     *  第一个位的位置为 0）。
     *
     * 注意：即使剩余字节数（count）小于一个完整的字长，
     * 此加载过程仍能正常工作。右侧以零补齐。 */
    c = (unsigned char*)l;
    for (j = 0; j < sizeof(*l); j++) {
        word <<= 8;
        if (count) {
            word |= *c;
            c++;
            count--;
        }
    }

    /* 特殊情况：
     * 如果字符串中所有位均为 0 且我们查找的是 1，
     * 则返回 -1，表示整个字符串中没有任何 "1" 位。
     * 当我们查找 "0" 时不会发生这种情况，因为我们假设字符串右侧为零填充。 */
    if (bit == 1 && word == 0) return -1;

    /* 处理最后一个字，逐位扫描。首先我们需要在一个 unsigned long
     * 的最高位构造一个单独的 "1"。由于我们不知道 long 的实际位数，
     * 这里使用一个简单的小技巧。 */
    one = ULONG_MAX; /* 所有位都置为 1 */
    one >>= 1;       /* 除最高位外所有位都置为 1 */
    one = ~one;      /* 仅最高位为 1，其余位为 0 */

    while(one) {
        if (((one & word) != 0) == bit) return pos;
        pos++;
        one >>= 1;
    }

    /* 正常情况下不应该执行到这里，因为无匹配的特殊情况已在前面处理过。
     * 如果执行到此处，说明算法存在 bug。 */
    serverPanic("End of redisBitpos() reached.");
    return 0; /* 仅用于避免编译器警告 */
}

/* 下面的 set.*Bitfield 与 get.*Bitfield 系列函数实现了在位图的任意位置
 * 读取或写入任意宽度（最高 64 位）的有符号或无符号整数。
 *
 * 位图表示规则：位号 0 对应首字节的最高有效位，依此类推。
 * 例如，将一个 5 位无符号整数设置为 23，写入偏移量 7 处（位图初始全为 0），
 * 将得到如下表示：
 *
 * +--------+--------+
 * |00000001|01110000|
 * +--------+--------+
 *
 * 当偏移量和整数宽度都按字节对齐时，这种表示与大端序相同；
 * 但当不对齐时，还需要理解字节内部的比特顺序。
 *
 * 注意：该格式与 SETBIT 以及相关命令的约定一致。
 */

/* 将一个无符号整数值写入位图的指定偏移处。
 * 参数：
 *   p      - 位图缓冲区指针
 *   offset - 起始位偏移
 *   bits   - 要写入的位数（<= 64）
 *   value  - 要写入的整数值 */
void setUnsignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits, uint64_t value) {
    uint64_t byte, bit, byteval, bitval, j;

    for (j = 0; j < bits; j++) {
        /* 从最高位开始依次取 value 的每一位 */
        bitval = (value & ((uint64_t)1<<(bits-1-j))) != 0;
        byte = offset >> 3;
        bit = 7 - (offset & 0x7);
        byteval = p[byte];
        /* 清除原值后写入新位值 */
        byteval &= ~(1 << bit);
        byteval |= bitval << bit;
        p[byte] = byteval & 0xff;
        offset++;
    }
}

/* 将一个有符号整数值写入位图的指定偏移处。
 * 对于负数，会通过补码转换为对应的无符号表示再写入。 */
void setSignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits, int64_t value) {
    uint64_t uv = value; /* 若 v 为负数，强制类型转换会加上 UINT64_MAX + 1 */
    setUnsignedBitfield(p,offset,bits,uv);
}

/* 从位图指定偏移处读取一个无符号整数。
 * 参数：
 *   p      - 位图缓冲区指针
 *   offset - 起始位偏移
 *   bits   - 要读取的位数（<= 64）
 * 返回值：读取到的无符号整数 */
uint64_t getUnsignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits) {
    uint64_t byte, bit, byteval, bitval, j, value = 0;

    for (j = 0; j < bits; j++) {
        byte = offset >> 3;
        bit = 7 - (offset & 0x7);
        byteval = p[byte];
        bitval = (byteval >> bit) & 1;
        value = (value<<1) | bitval;
        offset++;
    }
    return value;
}

/* 从位图指定偏移处读取一个有符号整数（采用补码表示）。 */
int64_t getSignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits) {
    int64_t value;
    union {uint64_t u; int64_t i;} conv;

    /* 当值不能完整表示时，从无符号到有符号的转换是未定义行为。
     * 不过此处假设使用二进制补码表示，并且原始值是通过有符号 -> 无符号
     * 转换得到的，因此如果原值是负数，最高位会被置 1。
     *
     * 注意：根据 C99 标准，定宽类型必须使用二进制补码。 */
    conv.u = getUnsignedBitfield(p,offset,bits);
    value = conv.i;

    /* 若最高有效位为 1，将其符号位扩展到更高的所有位，
     * 以得到有符号整数的补码表示。 */
    if (bits < 64 && (value & ((uint64_t)1 << (bits-1))))
        value |= ((uint64_t)-1) << bits;
    return value;
}

/* 以下两个函数用于检测将值存储为指定位数的有符号或无符号整数时是否溢出。
 * 函数接受当前值和可能的增量（increment）。
 * 如果不会发生溢出，且 value + incr 处于合法范围内，则返回 0；
 * 否则上溢时返回 1，下溢时返回 -1。
 *
 * 当返回非零值（发生溢出或下溢）时，如果 limit 参数不为 NULL，
 * 会根据指定的溢出语义设置 *limit 为操作应该得到的值：
 *
 * 对于 BFOVERFLOW_SAT：返回 1 时，*limit 被设置为该整数能表示的最大值；
 *                      返回 -1 时，*limit 被设置为该整数能表示的最小值。
 *
 * 对于 BFOVERFLOW_WRAP：*limit 通过执行运算设置，无符号整数回绕到零，
 *                      有符号整数回绕到能表示的最负数。 */

/* 位域溢出处理模式：WRAP（回绕） */
#define BFOVERFLOW_WRAP 0
/* 位域溢出处理模式：SAT（饱和，钳制到最大/最小值） */
#define BFOVERFLOW_SAT 1
/* 位域溢出处理模式：FAIL（失败，不写入），用于 BITFIELD 命令实现 */
#define BFOVERFLOW_FAIL 2

/* 检查无符号位域运算（value + incr）是否会溢出。
 * 返回值：0 正常；1 上溢；-1 下溢 */
int checkUnsignedBitfieldOverflow(uint64_t value, int64_t incr, uint64_t bits, int owtype, uint64_t *limit) {
    uint64_t max = (bits == 64) ? UINT64_MAX : (((uint64_t)1<<bits)-1);
    int64_t maxincr = max-value;
    int64_t minincr = -value;

    /* 上溢检测：若 value + incr 超过最大值 */
    if (value > max || (incr > 0 && incr > maxincr)) {
        if (limit) {
            if (owtype == BFOVERFLOW_WRAP) {
                goto handle_wrap;
            } else if (owtype == BFOVERFLOW_SAT) {
                *limit = max;
            }
        }
        return 1;
    } else if (incr < 0 && incr < minincr) {
        if (limit) {
            if (owtype == BFOVERFLOW_WRAP) {
                goto handle_wrap;
            } else if (owtype == BFOVERFLOW_SAT) {
                *limit = 0;
            }
        }
        return -1;
    }
    return 0;

handle_wrap:
    {
        /* WRAP 模式：执行实际的回绕计算，结果截断到指定位数 */
        uint64_t mask = ((uint64_t)-1) << bits;
        uint64_t res = value+incr;

        res &= ~mask;
        *limit = res;
    }
    return 1;
}

/* 检查有符号位域运算（value + incr）是否会溢出。
 * 返回值：0 正常；1 上溢；-1 下溢 */
int checkSignedBitfieldOverflow(int64_t value, int64_t incr, uint64_t bits, int owtype, int64_t *limit) {
    int64_t max = (bits == 64) ? INT64_MAX : (((int64_t)1<<(bits-1))-1);
    int64_t min = (-max)-1;

    /* Note that maxincr and minincr could overflow, but we use the values
     * only after checking 'value' range, so when we use it no overflow
     * happens. 'uint64_t' cast is there just to prevent undefined behavior on
     * overflow */
    int64_t maxincr = (uint64_t)max-value;
    int64_t minincr = min-value;

    if (value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr))
    {
        if (limit) {
            if (owtype == BFOVERFLOW_WRAP) {
                goto handle_wrap;
            } else if (owtype == BFOVERFLOW_SAT) {
                *limit = max;
            }
        }
        return 1;
    } else if (value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr)) {
        if (limit) {
            if (owtype == BFOVERFLOW_WRAP) {
                goto handle_wrap;
            } else if (owtype == BFOVERFLOW_SAT) {
                *limit = min;
            }
        }
        return -1;
    }
    return 0;

handle_wrap:
    {
        uint64_t msb = (uint64_t)1 << (bits-1);
        uint64_t a = value, b = incr, c;
        c = a+b; /* 按无符号方式执行加法，避免未定义行为 */

        /* 若符号位被置位，将其传播到所有更高位以封顶为负值；
         * 若符号位为 0，则屏蔽到正整数上限。 */
        if (bits < 64) {
            uint64_t mask = ((uint64_t)-1) << bits;
            if (c & msb) {
                c |= mask;
            } else {
                c &= ~mask;
            }
        }
        *limit = c;
    }
    return 1;
}

/* 调试函数：以人类可读的方式打印指定位图的内容。
 * 该函数当前未被调用，保留它的目的是在调试时无需重新编写。 */
void printBits(unsigned char *p, unsigned long count) {
    unsigned long j, i, byte;

    for (j = 0; j < count; j++) {
        byte = p[j];
        for (i = 0x80; i > 0; i /= 2)
            printf("%c", (byte & i) ? '1' : '0');
        printf("|");
    }
    printf("\n");
}

/* -----------------------------------------------------------------------------
 * 与位相关的字符串命令：GETBIT、SETBIT、BITCOUNT、BITOP。
 * -------------------------------------------------------------------------- */

/* BITOP 命令的操作类型：按位与 */
#define BITOP_AND   0
/* BITOP 命令的操作类型：按位或 */
#define BITOP_OR    1
/* BITOP 命令的操作类型：按位异或 */
#define BITOP_XOR   2
/* BITOP 命令的操作类型：按位取反 */
#define BITOP_NOT   3

/* BITFIELD 子命令类型：读取位域 */
#define BITFIELDOP_GET 0
/* BITFIELD 子命令类型：设置位域 */
#define BITFIELDOP_SET 1
/* BITFIELD 子命令类型：位域自增 */
#define BITFIELDOP_INCRBY 2

/* GETBIT / SETBIT 命令使用的辅助函数，用于解析位偏移参数。
 * 当偏移为负数或者超过 Redis 字符串值的 512 MB 限制
 * （即 server.proto_max_bulk_len）时，会向客户端返回错误。
 *
 * 如果 'hash' 参数为 true 且 'bits' 大于 0，那么函数还会解析以 "#"
 * 为前缀的位偏移。这种情况下，偏移值会乘以 'bits'。
 * 这对 BITFIELD 命令非常有用。
 * 时间复杂度：O(1) */
int getBitOffsetFromArgument(client *c, robj *o, uint64_t *offset, int hash, int bits) {
    long long loffset;
    char *err = "bit offset is not an integer or out of range";
    char *p = o->ptr;
    size_t plen = sdslen(p);
    int usehash = 0;

    /* 处理 #<offset> 形式：按位域宽度解释偏移 */
    if (p[0] == '#' && hash && bits > 0) usehash = 1;

    if (string2ll(p+usehash,plen-usehash,&loffset) == 0) {
        addReplyError(c,err);
        return C_ERR;
    }

    /* 对 #<offset> 形式，按位域宽度 bits 调整偏移 */
    if (usehash) loffset *= bits;

    /* 将偏移限制在 server.proto_max_bulk_len 之内（默认 512MB） */
    if (loffset < 0 || (!mustObeyClient(c) && (loffset >> 3) >= server.proto_max_bulk_len))
    {
        addReplyError(c,err);
        return C_ERR;
    }

    *offset = loffset;
    return C_OK;
}

/* BITFIELD 的辅助函数，用于解析位域类型字符串。
 * 类型格式为 <sign><bits>：
 *   sign  - 'u' 表示无符号，'i' 表示有符号
 *   bits  - 位宽，取值范围 1~64
 * 注意：64 位无符号整数会被报错，因为当前 Redis 协议无法返回
 *       大于 INT64_MAX 的无符号整数值。
 *
 * 出错时返回 C_ERR 并向客户端发送错误信息。 */
int getBitfieldTypeFromArgument(client *c, robj *o, int *sign, int *bits) {
    char *p = o->ptr;
    char *err = "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";
    long long llbits;

    if (p[0] == 'i') {
        *sign = 1;
    } else if (p[0] == 'u') {
        *sign = 0;
    } else {
        addReplyError(c,err);
        return C_ERR;
    }

    if ((string2ll(p+1,strlen(p+1),&llbits)) == 0 ||
        llbits < 1 ||
        (*sign == 1 && llbits > 64) ||
        (*sign == 0 && llbits > 63))
    {
        addReplyError(c,err);
        return C_ERR;
    }
    *bits = llbits;
    return C_OK;
}

/* 写位命令的辅助函数：用于将位写入字符串对象的命令实现。
 * 它会创建新对象，或在必要时用 0 填充现有对象，
 * 使其可以寻址到 'maxbit' 位，并最终返回该对象。
 * 如果键的类型不匹配，则返回 NULL 并向客户端发送错误信息。
 * 'dirty' 若不为 NULL，则在对象被新建或长度发生变化时被设置为 1。 */
robj *lookupStringForBitCommand(client *c, uint64_t maxbit, int *dirty) {
    size_t byte = maxbit >> 3;
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return NULL;
    if (dirty) *dirty = 0;

    if (o == NULL) {
        /* 键不存在：创建足够长的新字符串对象 */
        o = createObject(OBJ_STRING,sdsnewlen(NULL, byte+1));
        dbAdd(c->db,c->argv[1],o);
        if (dirty) *dirty = 1;
    } else {
        /* 键存在：取消共享并在需要时进行扩展 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        size_t oldlen = sdslen(o->ptr);
        o->ptr = sdsgrowzero(o->ptr,byte+1);
        if (dirty && oldlen != sdslen(o->ptr)) *dirty = 1;
    }
    return o;
}

/* 返回字符串对象内容的指针，并把长度写入 'len'。
 * 调用方需要提供（通常是栈上分配的）至少 LONG_STR_SIZE 字节的 'llbuf' 缓冲区。
 * 该缓冲区用于对象为整数编码的情况，
 * 以便在不进行堆分配的情况下提供其字符串表示。
 *
 * 函数返回指向对象底层字节数组的指针，可能是 'llbuf'，
 * 也可能是对象内部的表示。副作用：'len' 被填充为缓冲区的长度。
 *
 * 如果源对象为 NULL，函数保证返回 NULL 并将 'len' 设为 0。 */
unsigned char *getObjectReadOnlyString(robj *o, long *len, char *llbuf) {
    serverAssert(!o || o->type == OBJ_STRING);
    unsigned char *p = NULL;

    /* 将 'p' 指向字符串内容。如果字符串是整数编码，
     * 则使用栈上分配的数组作为临时存储。 */
    if (o && o->encoding == OBJ_ENCODING_INT) {
        p = (unsigned char*) llbuf;
        if (len) *len = ll2string(llbuf,LONG_STR_SIZE,(long)o->ptr);
    } else if (o) {
        p = (unsigned char*) o->ptr;
        if (len) *len = sdslen(o->ptr);
    } else {
        if (len) *len = 0;
    }
    return p;
}

/* SETBIT 命令实现。
 * 语法：SETBIT key offset bitvalue
 * 在 key 对应的字符串中，将 offset 指定的位设置为 bitvalue（0 或 1）。
 * 若 offset 超出当前字符串长度，字符串会自动扩展，中间填充 0。
 * 返回值：该位在修改前的原始值（0 或 1）。
 * 时间复杂度：O(1) */
void setbitCommand(client *c) {
    robj *o;
    char *err = "bit is not an integer or out of range";
    uint64_t bitoffset;
    ssize_t byte, bit;
    int byteval, bitval;
    long on;

    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset,0,0) != C_OK)
        return;

    if (getLongFromObjectOrReply(c,c->argv[3],&on,err) != C_OK)
        return;

    /* 位值只能是 0 或 1，其他值是非法的 */
    if (on & ~1) {
        addReplyError(c,err);
        return;
    }

    int dirty;
    if ((o = lookupStringForBitCommand(c,bitoffset,&dirty)) == NULL) return;

    /* 取出当前位所在的字节及该位在字节内的位置 */
    byte = bitoffset >> 3;
    byteval = ((uint8_t*)o->ptr)[byte];
    bit = 7 - (bitoffset & 0x7);
    bitval = byteval & (1 << bit);

    /* 仅在以下情况下才需要写入：
     * - 对象是新创建的；
     * - 对象长度发生了变化；
     * - 或者写入前后的位值不同。
     * 注意：这里的 bitval 实际上是一个十进制数（即掩码），
     * 因此需要使用 `!!` 将其转换为 0 或 1 以便与 on 比较。 */
    if (dirty || (!!bitval != on)) {
        /* 用新位值更新对应字节 */
        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        ((uint8_t*)o->ptr)[byte] = byteval;
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"setbit",c->argv[1],c->db->id);
        server.dirty++;
    }

    /* 返回该位的原始值（修改前的值） */
    addReply(c, bitval ? shared.cone : shared.czero);
}

/* GETBIT 命令实现。
 * 语法：GETBIT key offset
 * 返回 key 对应字符串中 offset 指定的位的值（0 或 1）。
 * 当 key 不存在或者 offset 超出当前字符串长度时，返回 0。
 * 时间复杂度：O(1) */
void getbitCommand(client *c) {
    robj *o;
    char llbuf[32];
    uint64_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;

    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset,0,0) != C_OK)
        return;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    byte = bitoffset >> 3;
    bit = 7 - (bitoffset & 0x7);
    if (sdsEncodedObject(o)) {
        /* 普通 SDS 编码对象：直接在底层字符串中按字节访问 */
        if (byte < sdslen(o->ptr))
            bitval = ((uint8_t*)o->ptr)[byte] & (1 << bit);
    } else {
        /* 整数编码对象：先转换为字符串表示再访问 */
        if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)o->ptr))
            bitval = llbuf[byte] & (1 << bit);
    }

    addReply(c, bitval ? shared.cone : shared.czero);
}

/* BITOP 命令实现。
 * 语法：BITOP op_name target_key src_key1 src_key2 ... src_keyN
 * 在一个或多个字符串之间执行位运算（AND / OR / XOR / NOT），
 * 并将结果保存到 target_key 中。
 * 返回值：保存到 target_key 中的字符串长度（字节数）。
 *
 * op_name: BITOP_AND / BITOP_OR / BITOP_XOR / BITOP_NOT
 * 注意：BITOP_NOT 是单目运算，只能有一个源 key。
 * 时间复杂度：O(N) */
REDIS_NO_SANITIZE("alignment")
void bitopCommand(client *c) {
    char *opname = c->argv[1]->ptr;
    robj *o, *targetkey = c->argv[2];
    unsigned long op, j, numkeys;
    robj **objects;      /* 源字符串对象数组 */
    unsigned char **src; /* 源字符串指针数组 */
    unsigned long *len, maxlen = 0; /* 源字符串长度数组及最大长度 */
    unsigned long minlen = 0;    /* 输入 key 中的最小长度 */
    unsigned char *res = NULL; /* 结果字符串 */

    /* 解析操作名（不区分大小写） */
    if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname,"and"))
        op = BITOP_AND;
    else if((opname[0] == 'o' || opname[0] == 'O') && !strcasecmp(opname,"or"))
        op = BITOP_OR;
    else if((opname[0] == 'x' || opname[0] == 'X') && !strcasecmp(opname,"xor"))
        op = BITOP_XOR;
    else if((opname[0] == 'n' || opname[0] == 'N') && !strcasecmp(opname,"not"))
        op = BITOP_NOT;
    else {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 一致性检查：BITOP NOT 是单目运算，只允许一个源 key */
    if (op == BITOP_NOT && c->argc != 4) {
        addReplyError(c,"BITOP NOT must be called with a single source key.");
        return;
    }

    /* 查找所有源 key，并将字符串对象指针保存到数组中 */
    numkeys = c->argc - 3;
    src = zmalloc(sizeof(unsigned char*) * numkeys);
    len = zmalloc(sizeof(long) * numkeys);
    objects = zmalloc(sizeof(robj*) * numkeys);
    for (j = 0; j < numkeys; j++) {
        o = lookupKeyRead(c->db,c->argv[j+3]);
        /* 不存在的 key 按空字符串处理 */
        if (o == NULL) {
            objects[j] = NULL;
            src[j] = NULL;
            len[j] = 0;
            minlen = 0;
            continue;
        }
        /* 若某个 key 不是字符串类型则返回错误 */
        if (checkType(c,o,OBJ_STRING)) {
            unsigned long i;
            for (i = 0; i < j; i++) {
                if (objects[i])
                    decrRefCount(objects[i]);
            }
            zfree(src);
            zfree(len);
            zfree(objects);
            return;
        }
        objects[j] = getDecodedObject(o);
        src[j] = objects[j]->ptr;
        len[j] = sdslen(objects[j]->ptr);
        if (len[j] > maxlen) maxlen = len[j];
        if (j == 0 || len[j] < minlen) minlen = len[j];
    }

    /* 当至少有一个输入字符串非空时，执行位运算 */
    if (maxlen) {
        res = (unsigned char*) sdsnewlen(NULL,maxlen);
        unsigned char output, byte;
        unsigned long i;

        /* 快速路径：当所有输入位图都有足够数据时，
         * 可以采用性能远高于朴素算法的快速路径。
         * 在 ARM 上跳过该快速路径，因为 GCC 会生成
         * 多字加载/存储指令，而这些指令在 ARM >= v6 上也不受支持。 */
        j = 0;
        #ifndef USE_ALIGNED_ACCESS
        if (minlen >= sizeof(unsigned long)*4 && numkeys <= 16) {
            unsigned long *lp[16];
            unsigned long *lres = (unsigned long*) res;

            memcpy(lp,src,sizeof(unsigned long*)*numkeys);
            memcpy(res,src[0],minlen);

            /* 为不同操作分别编写分支以提升速度 */
            if (op == BITOP_AND) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] &= lp[i][0];
                        lres[1] &= lp[i][1];
                        lres[2] &= lp[i][2];
                        lres[3] &= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_OR) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] |= lp[i][0];
                        lres[1] |= lp[i][1];
                        lres[2] |= lp[i][2];
                        lres[3] |= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_XOR) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] ^= lp[i][0];
                        lres[1] ^= lp[i][1];
                        lres[2] ^= lp[i][2];
                        lres[3] ^= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_NOT) {
                while(minlen >= sizeof(unsigned long)*4) {
                    lres[0] = ~lres[0];
                    lres[1] = ~lres[1];
                    lres[2] = ~lres[2];
                    lres[3] = ~lres[3];
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            }
        }
        #endif

        /* j 表示下一个待处理的字节，由上面的快速路径循环设置 */
        for (; j < maxlen; j++) {
            output = (len[0] <= j) ? 0 : src[0][j];
            if (op == BITOP_NOT) output = ~output;
            for (i = 1; i < numkeys; i++) {
                int skip = 0;
                byte = (len[i] <= j) ? 0 : src[i][j];
                switch(op) {
                case BITOP_AND:
                    output &= byte;
                    /* AND：一旦结果为 0，后续位也会是 0，可提前跳过 */
                    skip = (output == 0);
                    break;
                case BITOP_OR:
                    output |= byte;
                    /* OR：一旦结果为 0xFF，后续位也会是 1，可提前跳过 */
                    skip = (output == 0xff);
                    break;
                case BITOP_XOR: output ^= byte; break;
                }

                if (skip) {
                    break;
                }
            }
            res[j] = output;
        }
    }
    /* 释放源对象及相关临时数组 */
    for (j = 0; j < numkeys; j++) {
        if (objects[j])
            decrRefCount(objects[j]);
    }
    zfree(src);
    zfree(len);
    zfree(objects);

    /* 将计算结果写入目标 key */
    if (maxlen) {
        o = createObject(OBJ_STRING,res);
        setKey(c,c->db,targetkey,o,0);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",targetkey,c->db->id);
        decrRefCount(o);
        server.dirty++;
    } else if (dbDelete(c->db,targetkey)) {
        /* 所有输入字符串都为空时，删除目标 key */
        signalModifiedKey(c,c->db,targetkey);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",targetkey,c->db->id);
        server.dirty++;
    }
    addReplyLongLong(c,maxlen); /* 返回结果字符串的长度（字节数） */
}

/* BITCOUNT 命令实现。
 * 语法：BITCOUNT key [start end [BIT|BYTE]]
 * 统计 key 对应字符串中，指定范围内被置 1 的位的数量。
 * start 和 end 可以为负数（从末尾开始计数）。
 * 索引单位默认为字节，可显式指定 BIT（按位）或 BYTE（按字节）。
 * 时间复杂度：O(N)，N 是范围的长度 */
void bitcountCommand(client *c) {
    robj *o;
    long long start, end;
    long strlen;
    unsigned char *p;
    char llbuf[LONG_STR_SIZE];
    int isbit = 0;
    unsigned char first_byte_neg_mask = 0, last_byte_neg_mask = 0;

    /* 解析 start/end 范围参数（如果有） */
    if (c->argc == 4 || c->argc == 5) {
        if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
            return;
        if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
            return;
        if (c->argc == 5) {
            if (!strcasecmp(c->argv[4]->ptr,"bit")) isbit = 1;
            else if (!strcasecmp(c->argv[4]->ptr,"byte")) isbit = 0;
            else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
        /* 查找 key 并检查类型 */
        o = lookupKeyRead(c->db, c->argv[1]);
        if (checkType(c, o, OBJ_STRING)) return;
        p = getObjectReadOnlyString(o,&strlen,llbuf);
        long long totlen = strlen;

        /* 确保不会发生溢出 */
        serverAssert(totlen <= LLONG_MAX >> 3);

        /* 转换负数索引 */
        if (start < 0 && end < 0 && start > end) {
            addReply(c,shared.czero);
            return;
        }
        if (isbit) totlen <<= 3;
        if (start < 0) start = totlen+start;
        if (end < 0) end = totlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= totlen) end = totlen-1;
        if (isbit && start <= end) {
            /* 在将位偏移转换为字节偏移之前，先为边界字节构造掩码，
             * 以便在最后剔除范围外的位。 */
            first_byte_neg_mask = ~((1<<(8-(start&7)))-1) & 0xFF;
            last_byte_neg_mask = (1<<(7-(end&7)))-1;
            start >>= 3;
            end >>= 3;
        }
    } else if (c->argc == 2) {
        /* 仅指定 key：默认统计整个字符串 */
        /* 查找 key 并检查类型 */
        o = lookupKeyRead(c->db, c->argv[1]);
        if (checkType(c, o, OBJ_STRING)) return;
        p = getObjectReadOnlyString(o,&strlen,llbuf);
        /* 整个字符串 */
        start = 0;
        end = strlen-1;
    } else {
        /* 语法错误：参数个数不合法 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 不存在的 key 直接返回 0 */
    if (o == NULL) {
        addReply(c, shared.czero);
        return;
    }

    /* 前置条件：end >= 0 且 end < strlen，
     * 因此唯一返回 0 的情形是 start > end。 */
    if (start > end) {
        addReply(c,shared.czero);
    } else {
        long bytes = (long)(end-start+1);
        long long count = redisPopcount(p+start,bytes);
        if (first_byte_neg_mask != 0 || last_byte_neg_mask != 0) {
            unsigned char firstlast[2] = {0, 0};
            /* 首字节和尾字节中可能包含范围外的位，需要将它们从统计中剔除。
             * 这里使用一个技巧：将范围外的位保留在 firstlast 中，
             * 然后从总数中减去这些位的计数。 */
            if (first_byte_neg_mask != 0) firstlast[0] = p[start] & first_byte_neg_mask;
            if (last_byte_neg_mask != 0) firstlast[1] = p[end] & last_byte_neg_mask;
            count -= redisPopcount(firstlast,2);
        }
        addReplyLongLong(c,count);
    }
}

/* BITPOS 命令实现。
 * 语法：BITPOS key bit [start [end [BIT|BYTE]]]
 * 返回 key 对应的字符串中，第一个等于指定 bit 值（0 或 1）的位的位置。
 * start/end 可以为负数；范围单位默认为字节，可显式指定 BIT（按位）。
 * 当 key 不存在时，按无限 0 比特数组处理：
 *   - 查找 0 位返回 0；
 *   - 查找 1 位返回 -1。
 * 时间复杂度：O(N)，N 是范围内字节数 */
void bitposCommand(client *c) {
    robj *o;
    long long start, end;
    long bit, strlen;
    unsigned char *p;
    char llbuf[LONG_STR_SIZE];
    int isbit = 0, end_given = 0;
    unsigned char first_byte_neg_mask = 0, last_byte_neg_mask = 0;

    /* 解析 bit 参数，明确要查找的是 0 还是 1 */
    if (getLongFromObjectOrReply(c,c->argv[2],&bit,NULL) != C_OK)
        return;
    if (bit != 0 && bit != 1) {
        addReplyError(c, "The bit argument must be 1 or 0.");
        return;
    }

    /* 解析 start/end 范围参数（如果有） */
    if (c->argc == 4 || c->argc == 5 || c->argc == 6) {
        if (getLongLongFromObjectOrReply(c,c->argv[3],&start,NULL) != C_OK)
            return;
        if (c->argc == 6) {
            if (!strcasecmp(c->argv[5]->ptr,"bit")) isbit = 1;
            else if (!strcasecmp(c->argv[5]->ptr,"byte")) isbit = 0;
            else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
        if (c->argc >= 5) {
            if (getLongLongFromObjectOrReply(c,c->argv[4],&end,NULL) != C_OK)
                return;
            end_given = 1;
        }

        /* 查找 key 并检查类型 */
        o = lookupKeyRead(c->db, c->argv[1]);
        if (checkType(c, o, OBJ_STRING)) return;
        p = getObjectReadOnlyString(o, &strlen, llbuf);

        /* 确保不会发生溢出 */
        long long totlen = strlen;
        serverAssert(totlen <= LLONG_MAX >> 3);

        if (c->argc < 5) {
            /* 用户未显式指定 end：根据单位自动延伸到字符串末尾 */
            if (isbit) end = (totlen<<3) + 7;
            else end = totlen-1;
        }

        if (isbit) totlen <<= 3;
        /* 转换负数索引 */
        if (start < 0) start = totlen+start;
        if (end < 0) end = totlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= totlen) end = totlen-1;
        if (isbit && start <= end) {
            /* 在将位偏移转换为字节偏移之前，先为边界字节构造掩码 */
            first_byte_neg_mask = ~((1<<(8-(start&7)))-1) & 0xFF;
            last_byte_neg_mask = (1<<(7-(end&7)))-1;
            start >>= 3;
            end >>= 3;
        }
    } else if (c->argc == 3) {
        /* 仅指定 key 和 bit：默认搜索整个字符串 */
        /* 查找 key 并检查类型 */
        o = lookupKeyRead(c->db, c->argv[1]);
        if (checkType(c,o,OBJ_STRING)) return;
        p = getObjectReadOnlyString(o,&strlen,llbuf);

        /* 整个字符串 */
        start = 0;
        end = strlen-1;
    } else {
        /* 语法错误：参数个数不合法 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 如果 key 不存在，从我们的视角看就是一个无限的 0 比特数组。
     * 因此查找第一个 0 位返回 0；查找第一个 1 位返回 -1。 */
    if (o == NULL) {
        addReplyLongLong(c, bit ? -1 : 0);
        return;
    }

    /* 空范围（start > end）不包含任何 0 或 1，返回 -1 */
    if (start > end) {
        addReplyLongLong(c, -1);
    } else {
        long bytes = end-start+1;
        long long pos;
        unsigned char tmpchar;
        if (first_byte_neg_mask) {
            /* 处理首字节：屏蔽掉范围外的位 */
            if (bit) tmpchar = p[start] & ~first_byte_neg_mask;
            else tmpchar = p[start] | first_byte_neg_mask;
            /* 特殊情况：只有一个字节时，还要考虑末字节掩码 */
            if (last_byte_neg_mask && bytes == 1) {
                if (bit) tmpchar = tmpchar & ~last_byte_neg_mask;
                else tmpchar = tmpchar | last_byte_neg_mask;
            }
            pos = redisBitpos(&tmpchar,1,bit);
            /* 若没有剩余字节，或者已经在范围内找到匹配位，则提前退出 */
            if (bytes == 1 || (pos != -1 && pos != 8)) goto result;
            start++;
            bytes--;
        }
        /* 处理中间完整字节段；如果最后一个字节没有范围内位，需要先排除它 */
        long curbytes = bytes - (last_byte_neg_mask ? 1 : 0);
        if (curbytes > 0) {
            pos = redisBitpos(p+start,curbytes,bit);
            /* 若没有剩余字节，或者已经在范围内找到匹配位，则提前退出 */
            if (bytes == curbytes || (pos != -1 && pos != (long long)curbytes<<3)) goto result;
            start += curbytes;
            bytes -= curbytes;
        }
        /* 处理尾字节：屏蔽掉范围外的位 */
        if (bit) tmpchar = p[end] & ~last_byte_neg_mask;
        else tmpchar = p[end] | last_byte_neg_mask;
        pos = redisBitpos(&tmpchar,1,bit);

    result:
        /* 当用户指定了精确范围（start-end）查找 0 位时，
         * 不能把范围右侧视为零填充（与未指定 end 时不同）。
         * 因此如果 redisBitpos() 返回的是范围外的第一个位，
         * 就向调用方返回 -1，表示指定范围内不存在任何 "0" 位。 */
        if (end_given && bit == 0 && pos == (long long)bytes<<3) {
            addReplyLongLong(c,-1);
            return;
        }
        if (pos != -1) pos += (long long)start<<3; /* 加上前面跳过的字节数 */
        addReplyLongLong(c,pos);
    }
}

/* BITFIELD 命令实现。
 * 语法：BITFIELD key subcommand-1 arg ... subcommand-2 arg ...
 *
 * 支持的子命令：
 *
 * GET <type> <offset>          读取指定位域的当前值
 * SET <type> <offset> <value> 将位域设置为给定值
 * INCRBY <type> <offset> <increment> 自增位域的值
 * OVERFLOW [WRAP|SAT|FAIL]     设置溢出处理策略
 *
 * 该函数同时实现 BITFIELD 与 BITFIELD_RO：
 * 当 flags 设置为 BITFIELD_FLAG_READONLY 时，仅允许 GET 子命令，
 * 使用其他子命令会返回错误。
 * 时间复杂度：O(N)，其中 N 是子命令数量 */
#define BITFIELD_FLAG_NONE      0
/* BITFIELD 调用标志：只读模式，仅允许 GET 子命令 */
#define BITFIELD_FLAG_READONLY  (1<<0)

/* BITFIELD 子命令的描述结构：记录一次位域操作的参数 */
struct bitfieldOp {
    uint64_t offset;    /* 位域偏移 */
    int64_t i64;        /* INCRBY 的增量或 SET 的值 */
    int opcode;         /* 操作码（GET / SET / INCRBY） */
    int owtype;         /* 溢出处理类型 */
    int bits;           /* 位域位宽 */
    int sign;           /* 是否为有符号操作 */
};

/* BITFIELD / BITFIELD_RO 命令的通用实现。
 * 当 flags 设置为 BITFIELD_FLAG_READONLY 时，仅允许 GET 子命令，
 * 其他子命令会返回错误。 */
void bitfieldGeneric(client *c, int flags) {
    robj *o;
    uint64_t bitoffset;
    int j, numops = 0, changes = 0, dirty = 0;
    struct bitfieldOp *ops = NULL; /* 待执行的操作数组 */
    int owtype = BFOVERFLOW_WRAP; /* 溢出处理类型，默认为 WRAP */
    int readonly = 1;
    uint64_t highest_write_offset = 0;

    /* 解析所有子命令及其参数 */
    for (j = 2; j < c->argc; j++) {
        int remargs = c->argc-j-1; /* 当前参数之后剩余的参数数量 */
        char *subcmd = c->argv[j]->ptr; /* 当前子命令名 */
        int opcode; /* 当前操作码 */
        long long i64 = 0;  /* 有符号 SET 值 */
        int sign = 0; /* 是有符号还是无符号类型？ */
        int bits = 0; /* 位域宽度（位） */

        if (!strcasecmp(subcmd,"get") && remargs >= 2)
            opcode = BITFIELDOP_GET;
        else if (!strcasecmp(subcmd,"set") && remargs >= 3)
            opcode = BITFIELDOP_SET;
        else if (!strcasecmp(subcmd,"incrby") && remargs >= 3)
            opcode = BITFIELDOP_INCRBY;
        else if (!strcasecmp(subcmd,"overflow") && remargs >= 1) {
            char *owtypename = c->argv[j+1]->ptr;
            j++;
            if (!strcasecmp(owtypename,"wrap"))
                owtype = BFOVERFLOW_WRAP;
            else if (!strcasecmp(owtypename,"sat"))
                owtype = BFOVERFLOW_SAT;
            else if (!strcasecmp(owtypename,"fail"))
                owtype = BFOVERFLOW_FAIL;
            else {
                addReplyError(c,"Invalid OVERFLOW type specified");
                zfree(ops);
                return;
            }
            continue;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            zfree(ops);
            return;
        }

        /* 解析所有操作共有的 type 和 offset 参数 */
        if (getBitfieldTypeFromArgument(c,c->argv[j+1],&sign,&bits) != C_OK) {
            zfree(ops);
            return;
        }

        if (getBitOffsetFromArgument(c,c->argv[j+2],&bitoffset,1,bits) != C_OK){
            zfree(ops);
            return;
        }

        if (opcode != BITFIELDOP_GET) {
            readonly = 0;
            /* 跟踪写操作能达到的最远位，用于后续预分配空间 */
            if (highest_write_offset < bitoffset + bits - 1)
                highest_write_offset = bitoffset + bits - 1;
            /* INCRBY 和 SET 子命令还需要再读取一个参数 */
            if (getLongLongFromObjectOrReply(c,c->argv[j+3],&i64,NULL) != C_OK){
                zfree(ops);
                return;
            }
        }

        /* 将本次操作追加到操作数组中 */
        ops = zrealloc(ops,sizeof(*ops)*(numops+1));
        ops[numops].offset = bitoffset;
        ops[numops].i64 = i64;
        ops[numops].opcode = opcode;
        ops[numops].owtype = owtype;
        ops[numops].bits = bits;
        ops[numops].sign = sign;
        numops++;

        j += 3 - (opcode == BITFIELDOP_GET);
    }

    if (readonly) {
        /* 只读访问：键不存在也允许，但类型不是字符串则报错 */
        o = lookupKeyRead(c->db,c->argv[1]);
        if (o != NULL && checkType(c,o,OBJ_STRING)) {
            zfree(ops);
            return;
        }
    } else {
        if (flags & BITFIELD_FLAG_READONLY) {
            /* BITFIELD_RO 不允许任何写操作 */
            zfree(ops);
            addReplyError(c, "BITFIELD_RO only supports the GET subcommand");
            return;
        }

        /* 写访问：先确保字符串足够长以容纳最远的写入位 */
        if ((o = lookupStringForBitCommand(c,
            highest_write_offset,&dirty)) == NULL) {
            zfree(ops);
            return;
        }
    }

    addReplyArrayLen(c,numops);

    /* 实际执行所有操作 */
    for (j = 0; j < numops; j++) {
        struct bitfieldOp *thisop = ops+j;

        /* 执行操作 */
        if (thisop->opcode == BITFIELDOP_SET ||
            thisop->opcode == BITFIELDOP_INCRBY)
        {
            /* SET 和 INCRBY：为了简单起见，二者共用同一段代码逻辑。
             * SET 的返回值是修改前的值，因此也需要先读取再写入。 */

            /* 有符号和无符号操作需要两段非常相似但不同的代码路径，
             * 因为读取/写入函数和所用变量类型不同。 */
            if (thisop->sign) {
                int64_t oldval, newval, wrapped, retval;
                int overflow;

                oldval = getSignedBitfield(o->ptr,thisop->offset,
                        thisop->bits);

                if (thisop->opcode == BITFIELDOP_INCRBY) {
                    overflow = checkSignedBitfieldOverflow(oldval,
                            thisop->i64,thisop->bits,thisop->owtype,&wrapped);
                    newval = overflow ? wrapped : oldval + thisop->i64;
                    retval = newval;
                } else {
                    newval = thisop->i64;
                    overflow = checkSignedBitfieldOverflow(newval,
                            0,thisop->bits,thisop->owtype,&wrapped);
                    if (overflow) newval = wrapped;
                    retval = oldval;
                }

                /* 若溢出且溢出策略为 "FAIL"，则不写入，并返回 NULL 表示失败 */
                if (!(overflow && thisop->owtype == BFOVERFLOW_FAIL)) {
                    addReplyLongLong(c,retval);
                    setSignedBitfield(o->ptr,thisop->offset,
                                      thisop->bits,newval);

                    if (dirty || (oldval != newval))
                        changes++;
                } else {
                    addReplyNull(c);
                }
            } else {
                /* 对 wrapped 进行显式初始化，避免 "-Wmaybe-uninitialized" 误报 */
                uint64_t oldval, newval, retval, wrapped = 0;
                int overflow;

                oldval = getUnsignedBitfield(o->ptr,thisop->offset,
                        thisop->bits);

                if (thisop->opcode == BITFIELDOP_INCRBY) {
                    newval = oldval + thisop->i64;
                    overflow = checkUnsignedBitfieldOverflow(oldval,
                            thisop->i64,thisop->bits,thisop->owtype,&wrapped);
                    if (overflow) newval = wrapped;
                    retval = newval;
                } else {
                    newval = thisop->i64;
                    overflow = checkUnsignedBitfieldOverflow(newval,
                            0,thisop->bits,thisop->owtype,&wrapped);
                    if (overflow) newval = wrapped;
                    retval = oldval;
                }
                /* 若溢出且溢出策略为 "FAIL"，则不写入，并返回 NULL 表示失败 */
                if (!(overflow && thisop->owtype == BFOVERFLOW_FAIL)) {
                    addReplyLongLong(c,retval);
                    setUnsignedBitfield(o->ptr,thisop->offset,
                                        thisop->bits,newval);

                    if (dirty || (oldval != newval))
                        changes++;
                } else {
                    addReplyNull(c);
                }
            }
        } else {
            /* GET 子命令 */
            unsigned char buf[9];
            long strlen = 0;
            unsigned char *src = NULL;
            char llbuf[LONG_STR_SIZE];

            if (o != NULL)
                src = getObjectReadOnlyString(o,&strlen,llbuf);

            /* 对 GET 操作采用一个小技巧：在执行读取之前，
             * 先把最多 9 个字节复制到本地缓冲区，
             * 这样即使位域跨越 64 位边界也能轻松处理。 */
            memset(buf,0,9);
            int i;
            uint64_t byte = thisop->offset >> 3;
            for (i = 0; i < 9; i++) {
                if (src == NULL || i+byte >= (uint64_t)strlen) break;
                buf[i] = src[i+byte];
            }

            /* 现在对已零填充的本地缓冲区执行位域读取 */
            if (thisop->sign) {
                int64_t val = getSignedBitfield(buf,thisop->offset-(byte*8),
                                            thisop->bits);
                addReplyLongLong(c,val);
            } else {
                uint64_t val = getUnsignedBitfield(buf,thisop->offset-(byte*8),
                                            thisop->bits);
                addReplyLongLong(c,val);
            }
        }
    }

    if (changes) {
        /* 如果存在修改，触发相应的通知并更新脏数据计数 */
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"setbit",c->argv[1],c->db->id);
        server.dirty += changes;
    }
    zfree(ops);
}

/* BITFIELD 命令入口：调用通用实现，不带只读标志 */
void bitfieldCommand(client *c) {
    bitfieldGeneric(c, BITFIELD_FLAG_NONE);
}

/* BITFIELD_RO 命令入口：调用通用实现，带只读标志 */
void bitfieldroCommand(client *c) {
    bitfieldGeneric(c, BITFIELD_FLAG_READONLY);
}
