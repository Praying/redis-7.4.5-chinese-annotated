/* hyperloglog.c - Redis HyperLogLog 概率性基数估算。
 * 本文件实现了该算法以及导出的 Redis 命令。
 *
 * Copyright (c) 2014-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

#include <stdint.h>
#include <math.h>

/* Redis HyperLogLog 实现基于以下思路：
 *
 * * 使用 [1] 中提出的 64 位哈希函数，以便估算大于 10^9 的基数，
 *   每个寄存器只需额外 1 位的成本。
 * * 使用 16384 个 6 位寄存器以获得较高的精度，每个 key 占用共 12k 字节。
 * * 使用 Redis 字符串数据类型。不会引入新的类型。
 * * 不会像 [1] 那样尝试压缩数据结构。所使用的算法也是 [2] 中的原始
 *   HyperLogLog 算法，唯一区别在于使用了 64 位哈希函数，因此不会像 [1]
 *   那样对接近 2^32 的值进行修正。
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * Redis 使用两种表示方式：
 *
 * 1) "稠密"表示：每个条目由一个 6 位整数表示。
 * 2) "稀疏"表示：使用游程编码压缩，适合以内存高效方式表示
 *    大量寄存器值为 0 的 HyperLogLog。
 *
 *
 * HLL 头部
 * ===
 *
 * 稠密和稀疏表示都有一个 16 字节的头部，如下所示：
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * 前 4 个字节是设置为 "HYLL" 字节的魔数字符串。
 * "E" 是一个字节的编码方式，当前设置为 HLL_DENSE 或 HLL_SPARSE。
 * N/U 是 3 个未使用的字节。
 *
 * "Cardin." 字段是一个以小端格式存储的 64 位整数，表示最近一次
 * 计算得到的基数。如果数据结构自上次计算以来未被修改，则可以
 * 复用该缓存值（这一点非常有用，因为 HLLADD 操作大概率不会
 * 修改实际的数据结构，因此估算的基数可以被复用）。
 *
 * 当缓存基数最高有效字节中的最高有效位被设置时，表示数据结构
 * 已被修改，不能复用缓存值，必须重新计算。
 *
 * 稠密表示
 * ===
 *
 * Redis 使用的稠密表示如下：
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * 6 位计数器从 LSB 到 MSB 依次编码，需要时使用后续字节。
 *
 * 稀疏表示
 * ===
 *
 * 稀疏表示使用游程编码对寄存器进行编码，由三种操作码组成：
 * 两种使用一个字节，一种使用两个字节。这些操作码分别称为
 * ZERO、XZERO 和 VAL。
 *
 * ZERO 操作码表示为 00xxxxxx。由六位 'xxxxxx' 表示的 6 位整数加 1，
 * 表示有 N 个寄存器被设置为 0。该操作码可以表示从 1 到 64 个
 * 连续寄存器被设置为 0。
 *
 * XZERO 操作码由两个字节 01xxxxxx yyyyyyyy 表示。由比特 'xxxxxx'
 * 作为最高有效位、'yyyyyyyy' 作为最低有效位组成的 14 位整数加 1，
 * 表示有 N 个寄存器被设置为 0。该操作码可以表示从 0 到 16384 个
 * 连续寄存器被设置为 0。
 *
 * VAL 操作码表示为 1vvvvvxx。它包含一个 5 位整数表示寄存器的值，
 * 以及一个 2 位整数表示被设置为该值 'vvvvv' 的连续寄存器数量。
 * 要获得值和游程长度，需要将整数 vvvvv 和 xx 分别加 1。
 * 该操作码可以表示从 1 到 32 的值，重复次数为 1 到 4 次。
 *
 * 稀疏表示无法表示值大于 32 的寄存器。然而在基数较小且稀疏表示
 * 仍然比稠密表示更节省内存的情况下，几乎不可能遇到这样的寄存器。
 * 一旦遇到，HLL 就会被转换为稠密表示。
 *
 * 稀疏表示是纯位置编码的。例如空 HLL 的稀疏表示仅为：XZERO:16384。
 *
 * 一个 HLL 仅在位置 1000、1020、1021 处有三个非零寄存器，
 * 分别设置为 2、3、3，则由以下三个操作码表示：
 *
 * XZERO:1000  （寄存器 0-999 被设置为 0）
 * VAL:2,1     （1 个寄存器被设置为值 2，即寄存器 1000）
 * ZERO:19     （寄存器 1001-1019 被设置为 0）
 * VAL:3,2     （2 个寄存器被设置为值 3，即寄存器 1020、1021）
 * XZERO:15362 （寄存器 1022-16383 被设置为 0）
 *
 * 在上述示例中，稀疏表示仅用 7 个字节而不是 12k 字节来表示
 * HLL 寄存器。一般来说，在低基数场景下，空间效率有大幅提升，
 * 但代价是 CPU 时间，因为稀疏表示的访问速度较慢。
 *
 * 下表展示了平均基数与所使用字节数的关系，每个基数 100 个样本
 * （当集合因寄存器值过大而无法表示时，使用稠密表示的大小作为样本）：
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * 稠密表示使用 12288 字节，因此在基数约 2000-3000 以内仍有显著
 * 的优势。对于更大的基数，更新稀疏表示所需的固定时间开销相对于
 * 内存节省而言并不划算。该实现从稀疏表示切换到稠密表示时，
 * 稀疏表示的最大长度通过 server.hll_sparse_max_bytes 定义进行配置。
 */

/* HyperLogLog 头部结构。
 * 编码后的 HLL 对象包含一个 8 字节的魔数和编码头部，
 * 紧接 registers 数组存储基数估计所需的寄存器值。 */
struct hllhdr {
    char magic[4];      /* "HYLL" */
    uint8_t encoding;   /* HLL_DENSE 或 HLL_SPARSE。 */
    uint8_t notused[3]; /* 保留供未来使用，必须为零。 */
    uint8_t card[8];    /* 缓存的基数，小端格式。 */
    uint8_t registers[]; /* 数据字节。 */
};

/* 缓存基数的最高有效位用于标识缓存值是否有效。 */
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1<<7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1<<7)) == 0)

#define HLL_P 14 /* P 越大，误差越小。 */
#define HLL_Q (64-HLL_P) /* 哈希值中用于确定前导零数的位数。 */
#define HLL_REGISTERS (1<<HLL_P) /* P=14 时为 16384 个寄存器。 */
#define HLL_P_MASK (HLL_REGISTERS-1) /* 用于寄存器索引的掩码。 */
#define HLL_BITS 6 /* 足以计数最多 63 个前导零。 */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)
#define HLL_HDR_SIZE sizeof(struct hllhdr)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))
#define HLL_DENSE 0 /* 稠密编码。 */
#define HLL_SPARSE 1 /* 稀疏编码。 */
#define HLL_RAW 255 /* 仅在内部使用，永不暴露给外部。 */
#define HLL_MAX_ENCODING 1

static char *invalid_hll_err = "-INVALIDOBJ Corrupted HLL object detected";

/* =========================== Low level bit macros ========================= */

/* 访问稠密表示的宏。
 *
 * 我们需要在一个 8 位字节数组中获取和设置 6 位的计数器。
 * 使用宏以确保代码被内联，因为速度至关重要，特别是在 HLLCOUNT 中
 * 需要一次性访问所有寄存器来计算近似基数时。出于同样的原因，
 * 我们也希望在此代码路径中避免使用条件判断。
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * 注意：在上述表示中，每个字节的最高有效位（MSB）在左侧。
 * 我们从 LSB 到 MSB 开始使用比特，然后继续使用下一个字节。
 *
 * 例如，我们想要访问位置 pos = 1 处的计数器（上图中的 "111111"）。
 *
 * 包含我们数据的第一个字节 b0 的索引为：
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- b0 处的字节
 *   +--------+
 *
 * 字节中第一位（从 LSB = 0 开始计数）的位置由下式给出：
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * 将 b0 右移 'fb' 位。
 *
 *   +--------+
 *   |11000000|  <- b0 的初始值
 *   |00000011|  <- 右移 6 位后
 *   +--------+
 *
 * 将 b1 左移 8-fb 位（2 位）：
 *
 *   +--------+
 *   |22221111|  <- b1 的初始值
 *   |22111100|  <- 左移 2 位后
 *   +--------+
 *
 * 对两个比特进行 OR 操作，最后与 111111（十进制 63）进行 AND 操作，
 * 以清除我们不感兴趣的高位：
 *
 *   +--------+
 *   |00000011|  <- b0 右移
 *   |22111100|  <- b1 左移
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63，即我们的值
 *   +--------+
 *
 * 让我们用另一个例子来尝试，比如 pos = 0。在这种情况下，
 * 6 位计数器实际上只包含在一个字节中。
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- b0 处的字节
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  因此右移 0 位（实际上没有移位），左移下一个字节 8 位，
 *  即使我们不使用它，但这具有清除比特的效果，使得 OR 操作后
 *  结果不会受到影响。
 *
 * -------------------------------------------------------------------------
 *
 * 设置寄存器稍微复杂一些，假设 'val' 是我们想要设置的值，
 * 已在正确的范围内。
 *
 * 我们需要两个步骤：一步清除比特，另一步按位 OR 上新的比特。
 *
 * 让我们尝试 'pos' = 1 的情况，因此 'b' 处的第一个字节为 0，
 *
 * 此时的 "fb" 为 6。
 *
 *   +--------+
 *   |11000000|  <- b0 处的字节
 *   +--------+
 *
 * 要创建一个 AND 掩码以清除该位置的比特，我们只需将掩码初始化为 63，
 * 左移 "fs" 位，最后对结果取反。
 *
 *   +--------+
 *   |00111111|  <- "mask" 初始为 63
 *   |11000000|  <- "mask" 左移 "ls" 位后
 *   |00111111|  <- "mask" 取反后
 *   +--------+
 *
 * 现在我们可以将 "b" 处的字节与掩码进行按位 AND，并将其与左移
 * "ls" 位的 "val" 进行按位 OR，以设置新的比特。
 *
 * 现在让我们关注下一个字节 b1：
 *
 *   +--------+
 *   |22221111|  <- b1 的初始值
 *   +--------+
 *
 * 要构建 AND 掩码，我们再次从 63 开始，将其右移 8-fb 位，然后取反。
 *
 *   +--------+
 *   |00111111|  <- "mask" 设置为 2&6-1
 *   |00001111|  <- "mask" 右移 8-fb = 2 位后
 *   |11110000|  <- "mask" 按位取反后
 *   +--------+
 *
 * 现在我们可以将其与 b+1 进行掩码以清除旧的比特，并与左移
 * "rs" 位的 "val" 进行按位 OR，以设置新的值。
 */

/* 注意：如果我们访问最后一个计数器，也会访问到数组外的 b+1 字节，
 * 但 sds 字符串始终具有隐式空终止符，因此该字节是存在的，
 * 我们可以跳过条件判断（或显式多分配 1 字节的需要）。 */

/* 将位置 'regnum' 处的寄存器值存入变量 'target'。
 * 'p' 是一个无符号字节数组。 */
#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* 将位置 'regnum' 处的寄存器值设置为 'val'。
 * 'p' 是一个无符号字节数组。 */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = (regnum)*HLL_BITS/8; \
    unsigned long _fb = (regnum)*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = (val); \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

/* 访问稀疏表示的宏。
 * 宏的参数应为 uint8_t 指针。 */
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
#define HLL_SPARSE_VAL_MAX_VALUE 32
#define HLL_SPARSE_VAL_MAX_LEN 4
#define HLL_SPARSE_ZERO_MAX_LEN 64
#define HLL_SPARSE_XZERO_MAX_LEN 16384
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)
#define HLL_ALPHA_INF 0.721347520444481703680 /* 常数 0.5/ln(2) */

/* ========================= HyperLogLog algorithm  ========================= */

/* 我们的哈希函数是 MurmurHash2 的 64 位版本。
 * 该函数为 Redis 进行了修改，以便在大端和小端架构上
 * 提供相同的结果（端序中立）。 */
REDIS_NO_SANITIZE("alignment")
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)key;
    const uint8_t *end = data + (len-(len&7));

    while(data != end) {
        uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
    #ifdef USE_ALIGNED_ACCESS
        memcpy(&k,data,sizeof(uint64_t));
    #else
        k = *((uint64_t*)data);
    #endif
#else
        k = (uint64_t) data[0];
        k |= (uint64_t) data[1] << 8;
        k |= (uint64_t) data[2] << 16;
        k |= (uint64_t) data[3] << 24;
        k |= (uint64_t) data[4] << 32;
        k |= (uint64_t) data[5] << 40;
        k |= (uint64_t) data[6] << 48;
        k |= (uint64_t) data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch(len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48; /* fall-thru */
    case 6: h ^= (uint64_t)data[5] << 40; /* fall-thru */
    case 5: h ^= (uint64_t)data[4] << 32; /* fall-thru */
    case 4: h ^= (uint64_t)data[3] << 24; /* fall-thru */
    case 3: h ^= (uint64_t)data[2] << 16; /* fall-thru */
    case 2: h ^= (uint64_t)data[1] << 8; /* fall-thru */
    case 1: h ^= (uint64_t)data[0];
            h *= m; /* fall-thru */
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* 给定要添加到 HyperLogLog 的字符串元素，返回该元素哈希的
 * 000..1 模式的长度。作为副作用，'regp' 被设置为该元素
 * 哈希到的寄存器索引。 */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, bit, index;
    int count;

    /* 从 HLL_REGISTERS 位（即对应于第一个不用作索引的位的 2 的幂）
     * 开始计算零的个数。最大连续位数为 64-P+1 = Q+1 位。
     *
     * 注意，结束零序列的最后一个 "1" 必须包含在计数中，因此
     * 如果我们找到 "001"，则计数为 3，最小可能的计数是完全没有零，
     * 仅在第一个位置有一个 1 比特，即计数为 1。
     *
     * 这看起来效率低下，但实际上在平均情况下，经过几次迭代后
     * 找到 1 的概率很高。 */
    hash = MurmurHash64A(ele,elesize,0xadc83b19ULL);
    index = hash & HLL_P_MASK; /* 寄存器索引。 */
    hash >>= HLL_P; /* 移除用于寻址寄存器的比特。 */
    hash |= ((uint64_t)1<<HLL_Q); /* 确保循环终止且计数 <= Q+1。 */
    bit = 1;
    count = 1; /* 初始化为 1，因为我们计数的是 "00000...1" 模式。 */
    while((hash & bit) == 0) {
        count++;
        bit <<= 1;
    }
    *regp = (int) index;
    return count;
}

/* ================== Dense representation implementation  ================== */

/* 底层函数：将稠密 HLL 寄存器在 'index' 位置设置为指定值，
 * 但前提是当前值小于 'count'。
 *
 * 'registers' 应有容纳 HLL_REGISTERS 个寄存器和右侧额外一个
 * 字节的空间。sds 字符串会自动满足此要求，因为它们是隐式
 * 以空字符终止的。
 *
 * 该函数始终成功，但是若操作导致近似基数发生改变，则返回 1。
 * 否则返回 0。 */
int hllDenseSet(uint8_t *registers, long index, uint8_t count) {
    uint8_t oldcount;

    HLL_DENSE_GET_REGISTER(oldcount,registers,index);
    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers,index,count);
        return 1;
    } else {
        return 0;
    }
}

/* 向稠密 hyperloglog 数据结构中"添加"元素。
 * 实际上并没有添加任何内容，但会在需要时将元素所属子集的
 * 最大 0 模式计数器递增。
 *
 * 这只是 hllDenseSet() 的一个包装，对元素进行哈希以获取
 * 索引和零游程计数。 */
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize) {
    long index;
    uint8_t count = hllPatLen(ele,elesize,&index);
    /* 若该元素产生更长的零序列，则更新寄存器。 */
    return hllDenseSet(registers,index,count);
}

/* 在稠密表示下计算寄存器直方图。 */
void hllDenseRegHisto(uint8_t *registers, int* reghisto) {
    int j;

    /* Redis 默认使用 16384 个 6 位的寄存器。代码可通过修改 define
     * 来支持其他值，但对于我们的目标值，我们采用展开循环的快速路径。 */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
                      r10, r11, r12, r13, r14, r15;
        for (j = 0; j < 1024; j++) {
            /* 每次迭代处理 16 个寄存器。 */
            r0 = r[0] & 63;
            r1 = (r[0] >> 6 | r[1] << 2) & 63;
            r2 = (r[1] >> 4 | r[2] << 4) & 63;
            r3 = (r[2] >> 2) & 63;
            r4 = r[3] & 63;
            r5 = (r[3] >> 6 | r[4] << 2) & 63;
            r6 = (r[4] >> 4 | r[5] << 4) & 63;
            r7 = (r[5] >> 2) & 63;
            r8 = r[6] & 63;
            r9 = (r[6] >> 6 | r[7] << 2) & 63;
            r10 = (r[7] >> 4 | r[8] << 4) & 63;
            r11 = (r[8] >> 2) & 63;
            r12 = r[9] & 63;
            r13 = (r[9] >> 6 | r[10] << 2) & 63;
            r14 = (r[10] >> 4 | r[11] << 4) & 63;
            r15 = (r[11] >> 2) & 63;

            reghisto[r0]++;
            reghisto[r1]++;
            reghisto[r2]++;
            reghisto[r3]++;
            reghisto[r4]++;
            reghisto[r5]++;
            reghisto[r6]++;
            reghisto[r7]++;
            reghisto[r8]++;
            reghisto[r9]++;
            reghisto[r10]++;
            reghisto[r11]++;
            reghisto[r12]++;
            reghisto[r13]++;
            reghisto[r14]++;
            reghisto[r15]++;

            r += 12;
        }
    } else {
        for(j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;
            HLL_DENSE_GET_REGISTER(reg,registers,j);
            reghisto[reg]++;
        }
    }
}

/* ================== Sparse representation implementation  ================= */

/* 将以稀疏表示作为输入的 HLL 转换为其稠密表示。两种表示都由
 * SDS 字符串表示，并且输入表示会被作为副作用释放。
 *
 * 如果稀疏表示有效，则该函数返回 C_OK，否则在表示损坏时
 * 返回 C_ERR。 */
int hllSparseToDense(robj *o) {
    sds sparse = o->ptr, dense;
    struct hllhdr *hdr, *oldhdr = (struct hllhdr*)sparse;
    int idx = 0, runlen, regval;
    uint8_t *p = (uint8_t*)sparse, *end = p+sdslen(sparse);
    int valid = 1;

    /* 如果表示已经是正确的形式则尽快返回。 */
    hdr = (struct hllhdr*) sparse;
    if (hdr->encoding == HLL_DENSE) return C_OK;

    /* 创建一个用零字节填充的合适大小的字符串。
     * 注意，缓存基数作为副作用被设置为 0，这恰好是
     * 空 HLL 的基数。 */
    dense = sdsnewlen(NULL,HLL_DENSE_SIZE);
    hdr = (struct hllhdr*) dense;
    *hdr = *oldhdr; /* 这将复制魔数和缓存基数。 */
    hdr->encoding = HLL_DENSE;

    /* 现在读取稀疏表示并相应地设置非零寄存器。 */
    p += HLL_HDR_SIZE;
    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            idx += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            idx += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            while(runlen--) {
                HLL_DENSE_SET_REGISTER(hdr->registers,idx,regval);
                idx++;
            }
            p++;
        }
    }

    /* 如果稀疏表示有效，我们期望找到 idx 被设置为 HLL_REGISTERS。 */
    if (!valid || idx != HLL_REGISTERS) {
        sdsfree(dense);
        return C_ERR;
    }

    /* 释放旧表示并设置新表示。 */
    sdsfree(o->ptr);
    o->ptr = dense;
    return C_OK;
}

/* 底层函数：将稀疏 HLL 寄存器在 'index' 位置设置为指定值，
 * 但前提是当前值小于 'count'。
 *
 * 对象 'o' 是保存 HLL 的 String 对象。该函数需要对对象的引用，
 * 以便在需要时能够扩大字符串。
 *
 * 成功时，若基数发生改变则返回 1；若该元素的寄存器未被更新
 * 则返回 0。出错时（表示无效）返回 -1。
 *
 * 作为副作用，该函数可能将 HLL 表示从稀疏提升到稠密：当寄存器
 * 需要被设置为稀疏表示无法表示的值时，或者当结果大小将大于
 * server.hll_sparse_max_bytes 时，就会发生这种情况。 */
int hllSparseSet(robj *o, long index, uint8_t count) {
    struct hllhdr *hdr;
    uint8_t oldcount, *sparse, *end, *p, *prev, *next;
    long first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;

    /* 如果计数太大而无法由稀疏表示表示，则切换到稠密表示。 */
    if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;

    /* 当更新稀疏表示时，有时在最坏情况下我们需要将缓冲区扩大
     * 最多 3 个字节（XZERO 拆分为 XZERO-VAL-XZERO），下面的
     * 代码负责执行扩展工作。
     * 实际上，我们采用贪婪策略，扩大超过 3 个字节以避免在
     * 增量增长时未来重新分配。但我们为稀疏表示分配的字节数
     * 不会超过 'server.hll_sparse_max_bytes'。
     * 如果 hyperloglog sds 字符串的可用大小不足以容纳我们
     * 所需的增量，我们将在'步骤 3'中把 hypreloglog 提升为稠密表示。
     */
    if (sdsalloc(o->ptr) < server.hll_sparse_max_bytes && sdsavail(o->ptr) < 3) {
        size_t newlen = sdslen(o->ptr) + 3;
        newlen += min(newlen, 300); /* 贪婪策略：当 newlen 小于 300 时将其翻倍，超过 300 时则加 300 */
        if (newlen > server.hll_sparse_max_bytes)
            newlen = server.hll_sparse_max_bytes;
        o->ptr = sdsResize(o->ptr, newlen, 1);
    }

    /* 步骤 1：我们需要定位需要修改的操作码，以检查是否真的
     * 需要进行值更新。 */
    sparse = p = ((uint8_t*)o->ptr) + HLL_HDR_SIZE;
    end = p + sdslen(o->ptr) - HLL_HDR_SIZE;

    first = 0;
    prev = NULL; /* 循环结束时指向上一个操作码。 */
    next = NULL; /* 循环结束时指向下一个操作码。 */
    span = 0;
    while(p < end) {
        long oplen;

        /* 将 span 设置为此操作码覆盖的寄存器数量。
         *
         * 这是稀疏表示中性能最关键的循环。在多字节稀疏 HLL 中，
         * 将条件判断按操作码出现频率从高到低排序会更快。 */
        oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else { /* XZERO。 */
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        /* 若此操作码覆盖的寄存器包含 'index'，则跳出循环。 */
        if (index <= first+span-1) break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0 || p >= end) return -1; /* 格式无效。 */

    next = HLL_SPARSE_IS_XZERO(p) ? p+2 : p+1;
    if (next >= end) next = NULL;

    /* 缓存当前操作码类型，以避免对不会发生变化的内容反复使用宏。
     * 同时缓存该操作码的游程长度。 */
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* 步骤 2：循环结束后：
     *
     * 'first' 存储由当前操作码（由 'p' 指向）覆盖的第一个
     *  寄存器的索引。
     *
     * 'next' 和 'prev' 分别存储下一个和上一个操作码；如果 'p' 处
     *  的操作码是最后一个或第一个，则分别为 NULL。
     *
     * 'span' 被设置为当前操作码覆盖的寄存器数量。
     *
     * 为了就地更新数据结构（而非从头重新生成），有以下几种情况：
     *
     * A) 如果是 VAL 操作码且其值已经 >= 我们的 'count'，
     *    则无需更新，无论 VAL 的游程长度字段如何。
     *    在这种情况下 PFADD 返回 0，因为没有执行任何更改。
     *
     * B) 如果是 len = 1 的 VAL 操作码（仅表示我们的寄存器）
     *    且其值小于 'count'，则直接更新，因为这是一个平凡的情况。 */
    if (is_val) {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* 情况 A。 */
        if (oldcount >= count) return 0;

        /* 情况 B。 */
        if (runlen == 1) {
            HLL_SPARSE_VAL_SET(p,count,1);
            goto updated;
        }
    }

    /* C) 另一个易于处理的情况是 len = 1 的 ZERO 操作码。
     * 我们可以直接用值为我们的值且 len = 1 的 VAL 操作码替换它。 */
    if (is_zero && runlen == 1) {
        HLL_SPARSE_VAL_SET(p,count,1);
        goto updated;
    }

    /* D) 一般情况。
     *
     * 其他情况更为复杂：我们的寄存器需要更新，并且它当前由
     * len > 1 的 VAL 操作码、len > 1 的 ZERO 操作码或 XZERO
     * 操作码表示。
     *
     * 在这些情况下，必须将原始操作码拆分为多个操作码。
     * 最坏的情况是 XZERO 在中间拆分为 XZERO - VAL - XZERO，
     * 因此结果序列的最大长度为 5 字节。
     *
     * 我们执行拆分，将新序列写入 'new' 缓冲区，长度为 'newlen'。
     * 之后将新序列插入到旧序列的位置，如果新序列比旧序列更长，
     * 可能需要将右侧的内容移动几个字节。 */
    uint8_t seq[5], *n = seq;
    int last = first+span-1; /* 该序列覆盖的最后一个寄存器。 */
    int len;

    if (is_zero || is_xzero) {
        /* 处理 ZERO / XZERO 的拆分。 */
        if (index != first) {
            len = index-first;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
    } else {
        /* 处理 VAL 的拆分。 */
        int curval = HLL_SPARSE_VAL_VALUE(p);

        if (index != first) {
            len = index-first;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
    }

    /* 步骤 3：用新序列替换旧序列。
     *
     * 注意我们已经通过调用 sdsResize() 在 sds 字符串上分配了空间。 */
    int seqlen = n-seq;
    int oldlen = is_xzero ? 2 : 1;
    int deltalen = seqlen-oldlen;

    if (deltalen > 0 &&
        sdslen(o->ptr) + deltalen > server.hll_sparse_max_bytes) goto promote;
    serverAssert(sdslen(o->ptr) + deltalen <= sdsalloc(o->ptr));
    if (deltalen && next) memmove(next+deltalen,next,end-next);
    sdsIncrLen(o->ptr,deltalen);
    memcpy(p,seq,seqlen);
    end += deltalen;

updated:
    /* 步骤 4：如果可能，合并相邻的值。
     *
     * 表示已被更新，但所得的表示可能不是最优的：相邻的 VAL
     * 操作码有时可以合并为单个操作码。 */
    p = prev ? prev : sparse;
    int scanlen = 5; /* 从 prev 开始最多扫描 5 个操作码。 */
    while (p < end && scanlen--) {
        if (HLL_SPARSE_IS_XZERO(p)) {
            p += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p)) {
            p++;
            continue;
        }
        /* 我们需要两个相邻的 VAL 操作码才能尝试合并，它们必须
         * 具有相同的值，且总长度不能超过 VAL 操作码的最大长度。 */
        if (p+1 < end && HLL_SPARSE_IS_VAL(p+1)) {
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p+1);
            if (v1 == v2) {
                int len = HLL_SPARSE_VAL_LEN(p)+HLL_SPARSE_VAL_LEN(p+1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    HLL_SPARSE_VAL_SET(p+1,v1,len);
                    memmove(p,p+1,end-p);
                    sdsIncrLen(o->ptr,-1);
                    end--;
                    /* 合并后我们重复循环但不递增 'p'，以便尝试
                     * 将刚刚合并的值与其右侧的值再次合并。 */
                    continue;
                }
            }
        }
        p++;
    }

    /* 使缓存的基数失效。 */
    hdr = o->ptr;
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

promote: /* 提升为稠密表示。 */
    if (hllSparseToDense(o) == C_ERR) return -1; /* HLL 已损坏。 */
    hdr = o->ptr;

    /* 我们需要在转换后调用 hllDenseAdd() 来执行该操作。
     * 然而结果必须为 1，因为如果我们需要从稀疏转换为稠密，
     * 那么寄存器就需要被更新。
     *
     * 注意，这反过来意味着 PFADD 将确保该命令传播到从节点/AOF，
     * 因此如果存在稀疏到稠密的转换，它也会在所有从节点上执行。 */
    int dense_retval = hllDenseSet(hdr->registers,index,count);
    serverAssert(dense_retval == 1);
    return dense_retval;
}

/* 向稀疏 hyperloglog 数据结构中"添加"元素。
 * 实际上并没有添加任何内容，但会在需要时将元素所属子集的
 * 最大 0 模式计数器递增。
 *
 * 该函数实际上是 hllSparseSet() 的一个包装，仅对元素执行哈希
 * 以获取索引和零游程长度。 */
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize) {
    long index;
    uint8_t count = hllPatLen(ele,elesize,&index);
    /* 若该元素产生更长的零序列，则更新寄存器。 */
    return hllSparseSet(o,index,count);
}

/* 在稀疏表示下计算寄存器直方图。 */
void hllSparseRegHisto(uint8_t *sparse, int sparselen, int *invalid, int* reghisto) {
    int idx = 0, runlen, regval;
    uint8_t *end = sparse+sparselen, *p = sparse;
    int valid = 1;

    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[0] += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[0] += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* 溢出。 */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[regval] += runlen;
            p++;
        }
    }
    if ((!valid || idx != HLL_REGISTERS) && invalid) *invalid = 1;
}

/* ========================= HyperLogLog Count ==============================
 * 这是算法的核心部分，用于计算近似基数。
 * 该函数使用底层函数 hllDenseRegHisto() 和 hllSparseRegHisto() 作为辅助
 * 来计算寄存器值的直方图，这是与表示相关的部分，而其他所有部分都是通用的。 */

/* 实现 uint8_t 数据类型的寄存器直方图计算，
 * 仅在内部用作多 key 的 PFCOUNT 的加速。 */
void hllRawRegHisto(uint8_t *registers, int* reghisto) {
    uint64_t *word = (uint64_t*) registers;
    uint8_t *bytes;
    int j;

    for (j = 0; j < HLL_REGISTERS/8; j++) {
        if (*word == 0) {
            reghisto[0] += 8;
        } else {
            bytes = (uint8_t*) word;
            reghisto[bytes[0]]++;
            reghisto[bytes[1]]++;
            reghisto[bytes[2]]++;
            reghisto[bytes[3]]++;
            reghisto[bytes[4]]++;
            reghisto[bytes[5]]++;
            reghisto[bytes[6]]++;
            reghisto[bytes[7]]++;
        }
        word++;
    }
}

/* 辅助函数 sigma，定义见：
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllSigma(double x) {
    if (x == 1.) return INFINITY;
    double zPrime;
    double y = 1;
    double z = x;
    do {
        x *= x;
        zPrime = z;
        z += x * y;
        y += y;
    } while(zPrime != z);
    return z;
}

/* 辅助函数 tau，定义见：
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllTau(double x) {
    if (x == 0. || x == 1.) return 0.;
    double zPrime;
    double y = 1.0;
    double z = 1 - x;
    do {
        x = sqrt(x);
        zPrime = z;
        y *= 0.5;
        z -= pow(1 - x, 2)*y;
    } while(zPrime != z);
    return z / 3;
}

/* 根据寄存器值的调和平均值返回集合的近似基数。
 * 'hdr' 指向 SDS 的起始位置，该 SDS 表示保存 HLL 表示的 String 对象。
 *
 * 如果 HLL 对象的稀疏表示无效，则由 'invalid' 指向的整数被设置为非零，
 * 否则保持不变。
 *
 * hllCount() 支持一种仅供内部使用的特殊编码 HLL_RAW，
 * 即 hdr->registers 将指向一个长度为 HLL_REGISTERS 元素的 uint8_t 数组。
 * 这对于在针对多个 key 调用 PFCOUNT 时进行加速非常有用
 * （无需处理 6 位整数编码）。 */
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;
    double E;
    int j;
    /* 注意 reghisto 数组的大小本可以仅为 HLL_Q+2，因为 HLL_Q+1 是
     * 哈希函数能够返回的 "000...1" 序列的最大频率。然而对输入进行
     * 健全性检查速度很慢：因此我们使用一个安全大小的 history 数组：
     * 溢出只会将数据写入错误但已正确分配的位置。 */
    int reghisto[64] = {0};

    /* 计算寄存器直方图 */
    if (hdr->encoding == HLL_DENSE) {
        hllDenseRegHisto(hdr->registers,reghisto);
    } else if (hdr->encoding == HLL_SPARSE) {
        hllSparseRegHisto(hdr->registers,
                         sdslen((sds)hdr)-HLL_HDR_SIZE,invalid,reghisto);
    } else if (hdr->encoding == HLL_RAW) {
        hllRawRegHisto(hdr->registers,reghisto);
    } else {
        serverPanic("Unknown HyperLogLog encoding in hllCount()");
    }

    /* 根据寄存器直方图估算基数。参见：
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    double z = m * hllTau((m-reghisto[HLL_Q+1])/(double)m);
    for (j = HLL_Q; j >= 1; --j) {
        z += reghisto[j];
        z *= 0.5;
    }
    z += m * hllSigma(reghisto[0]/(double)m);
    E = llroundl(HLL_ALPHA_INF*m*m/z);

    return (uint64_t) E;
}

/* 根据 HLL 编码调用 hllDenseAdd() 或 hllSparseAdd()。 */
int hllAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr = o->ptr;
    switch(hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers,ele,elesize);
    case HLL_SPARSE: return hllSparseAdd(o,ele,elesize);
    default: return -1; /* 表示无效。 */
    }
}

/* 通过计算 MAX(registers[i],hll[i])，将 HyperLogLog 'hll' 与由 'max'
 * 指向的 uint8_t HLL_REGISTERS 寄存器数组合并。
 *
 * hll 对象必须已通过 isHLLObjectOrReply() 或其他方式进行验证。
 *
 * 如果 HyperLogLog 是稀疏的且发现无效，则返回 C_ERR，
 * 否则该函数始终成功。 */
int hllMerge(uint8_t *max, robj *hll) {
    struct hllhdr *hdr = hll->ptr;
    int i;

    if (hdr->encoding == HLL_DENSE) {
        uint8_t val;

        for (i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val > max[i]) max[i] = val;
        }
    } else {
        uint8_t *p = hll->ptr, *end = p + sdslen(hll->ptr);
        long runlen, regval;
        int valid = 1;

        p += HLL_HDR_SIZE;
        i = 0;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                if ((runlen + i) > HLL_REGISTERS) { /* 溢出。 */
                    valid = 0;
                    break;
                }
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                if ((runlen + i) > HLL_REGISTERS) { /* 溢出。 */
                    valid = 0;
                    break;
                }
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + i) > HLL_REGISTERS) { /* 溢出。 */
                    valid = 0;
                    break;
                }
                while(runlen--) {
                    if (regval > max[i]) max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (!valid || i != HLL_REGISTERS) return C_ERR;
    }
    return C_OK;
}

/* ========================== HyperLogLog commands ========================== */

/* 创建一个 HLL 对象。我们始终使用稀疏编码创建 HLL。
 * 之后将根据需要升级为稠密表示。 */
robj *createHLLObject(void) {
    robj *o;
    struct hllhdr *hdr;
    sds s;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE +
                    (((HLL_REGISTERS+(HLL_SPARSE_XZERO_MAX_LEN-1)) /
                     HLL_SPARSE_XZERO_MAX_LEN)*2);
    int aux;

    /* 用足够的 XZERO 操作码填充稀疏表示以表示所有寄存器。 */
    aux = HLL_REGISTERS;
    s = sdsnewlen(NULL,sparselen);
    p = (uint8_t*)s + HLL_HDR_SIZE;
    while(aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) xzero = aux;
        HLL_SPARSE_XZERO_SET(p,xzero);
        p += 2;
        aux -= xzero;
    }
    serverAssert((p-(uint8_t*)s) == sparselen);

    /* 创建实际的对象。 */
    o = createObject(OBJ_STRING,s);
    hdr = o->ptr;
    memcpy(hdr->magic,"HYLL",4);
    hdr->encoding = HLL_SPARSE;
    return o;
}

/* 检查对象是否是具有有效 HLL 表示的 String。
 * 如果是则返回 C_OK，否则向客户端回复一个错误并返回 C_ERR。 */
int isHLLObjectOrReply(client *c, robj *o) {
    struct hllhdr *hdr;

    /* Key 存在，检查类型 */
    if (checkType(c,o,OBJ_STRING))
        return C_ERR; /* 错误已发送。 */

    if (!sdsEncodedObject(o)) goto invalid;
    if (stringObjectLen(o) < sizeof(*hdr)) goto invalid;
    hdr = o->ptr;

    /* 魔数应为 "HYLL"。 */
    if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' ||
        hdr->magic[2] != 'L' || hdr->magic[3] != 'L') goto invalid;

    if (hdr->encoding > HLL_MAX_ENCODING) goto invalid;

    /* 稠密表示的字符串长度应完全匹配。 */
    if (hdr->encoding == HLL_DENSE &&
        stringObjectLen(o) != HLL_DENSE_SIZE) goto invalid;

    /* 所有检查通过。 */
    return C_OK;

invalid:
    addReplyError(c,"-WRONGTYPE Key is not a valid "
               "HyperLogLog string value.");
    return C_ERR;
}

/* PFADD var ele ele ele ... ele => :0 或 :1 */
void pfaddCommand(client *c) {
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    struct hllhdr *hdr;
    int updated = 0, j;

    if (o == NULL) {
        /* 使用精确长度的字符串值创建 key，以容纳我们的 HLL 数据结构。
         * 当传入 NULL 时，sdsnewlen() 保证返回的字节已初始化为零。 */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
        updated++;
    } else {
        if (isHLLObjectOrReply(c,o) != C_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }
    /* 对每个元素执行底层 ADD 操作。 */
    for (j = 2; j < c->argc; j++) {
        int retval = hllAdd(o, (unsigned char*)c->argv[j]->ptr,
                               sdslen(c->argv[j]->ptr));
        switch(retval) {
        case 1:
            updated++;
            break;
        case -1:
            addReplyError(c,invalid_hll_err);
            return;
        }
    }
    hdr = o->ptr;
    if (updated) {
        HLL_INVALIDATE_CACHE(hdr);
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
        server.dirty += updated;
    }
    addReply(c, updated ? shared.cone : shared.czero);
}

/* PFCOUNT var -> 集合的近似基数。 */
void pfcountCommand(client *c) {
    robj *o;
    struct hllhdr *hdr;
    uint64_t card;

    /* 情况 1：多 key，集合并集的基数。
     *
     * 当指定多个 key 时，PFCOUNT 实际计算的是指定的 N 个 HLL
     * 合并后的基数。 */
    if (c->argc > 2) {
        uint8_t max[HLL_HDR_SIZE+HLL_REGISTERS], *registers;
        int j;

        /* 计算一个 HLL，其 M[i] = MAX(M[i]_j)。 */
        memset(max,0,sizeof(max));
        hdr = (struct hllhdr*) max;
        hdr->encoding = HLL_RAW; /* 仅供内部使用的特殊编码。 */
        registers = max + HLL_HDR_SIZE;
        for (j = 1; j < c->argc; j++) {
            /* 检查类型和大小。 */
            robj *o = lookupKeyRead(c->db,c->argv[j]);
            if (o == NULL) continue; /* 对不存在的变量视为空 HLL。 */
            if (isHLLObjectOrReply(c,o) != C_OK) return;

            /* 将此 HLL 与我们的 'max' HLL 合并，将 max[i] 设置为
             * MAX(max[i],hll[i])。 */
            if (hllMerge(registers,o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
        }

        /* 计算结果集合的基数。 */
        addReplyLongLong(c,hllCount(hdr,NULL));
        return;
    }

    /* 情况 2：单个 HLL 的基数。
     *
     * 用户指定了单个 key。返回缓存值，或者计算并更新缓存。
     *
     * 由于 HLL 是普通的 Redis 字符串类型值，更新缓存确实会修改值。
     * 我们仍然使用 lookupKeyRead 因为该命令被标记为只读命令。
     * 区别在于：使用 lookupKeyWrite 时，从节点上逻辑过期的 key
     * 会被删除，而使用 lookupKeyRead 时不会；但如果 key 已逻辑过期，
     * 两种查找方式都会返回 NULL，这才是这里关心的。 */
    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        /* 没有 key？基数为零，因为没有添加任何元素，否则
         * 我们会拥有一个 HLLADD 作为副作用创建的 key。 */
        addReply(c,shared.czero);
    } else {
        if (isHLLObjectOrReply(c,o) != C_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);

        /* 检查缓存的基数是否有效。 */
        hdr = o->ptr;
        if (HLL_VALID_CACHE(hdr)) {
            /* 直接返回缓存值。 */
            card = (uint64_t)hdr->card[0];
            card |= (uint64_t)hdr->card[1] << 8;
            card |= (uint64_t)hdr->card[2] << 16;
            card |= (uint64_t)hdr->card[3] << 24;
            card |= (uint64_t)hdr->card[4] << 32;
            card |= (uint64_t)hdr->card[5] << 40;
            card |= (uint64_t)hdr->card[6] << 48;
            card |= (uint64_t)hdr->card[7] << 56;
        } else {
            int invalid = 0;
            /* 重新计算并更新缓存值。 */
            card = hllCount(hdr,&invalid);
            if (invalid) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            hdr->card[0] = card & 0xff;
            hdr->card[1] = (card >> 8) & 0xff;
            hdr->card[2] = (card >> 16) & 0xff;
            hdr->card[3] = (card >> 24) & 0xff;
            hdr->card[4] = (card >> 32) & 0xff;
            hdr->card[5] = (card >> 40) & 0xff;
            hdr->card[6] = (card >> 48) & 0xff;
            hdr->card[7] = (card >> 56) & 0xff;
            /* 这被认为是一个只读命令，即使缓存值可能被修改，
             * 并且由于 HLL 是 Redis 字符串，我们需要传播该更改。 */
            signalModifiedKey(c,c->db,c->argv[1]);
            server.dirty++;
        }
        addReplyLongLong(c,card);
    }
}

/* PFMERGE dest src1 src2 src3 ... srcN => OK */
void pfmergeCommand(client *c) {
    uint8_t max[HLL_REGISTERS];
    struct hllhdr *hdr;
    int j;
    int use_dense = 0; /* 是否将稠密表示作为目标？ */

    /* 计算一个 HLL，其 M[i] = MAX(M[i]_j)。
     * 我们将最大值存储到 max 寄存器数组中，稍后再将其写入目标变量。 */
    memset(max,0,sizeof(max));
    for (j = 1; j < c->argc; j++) {
        /* 检查类型和大小。 */
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) continue; /* 对不存在的变量视为空 HLL。 */
        if (isHLLObjectOrReply(c,o) != C_OK) return;

        /* 如果至少有一个涉及的 HLL 是稠密的，则尽快使用稠密表示
         * 作为目标，以节省时间并避免转换步骤。 */
        hdr = o->ptr;
        if (hdr->encoding == HLL_DENSE) use_dense = 1;

        /* 将此 HLL 与我们的 'max' HLL 合并，将 max[i] 设置为
         * MAX(max[i],hll[i])。 */
        if (hllMerge(max,o) == C_ERR) {
            addReplyError(c,invalid_hll_err);
            return;
        }
    }

    /* 如有需要，创建/解共享目标 key 的值。 */
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* 使用精确长度的字符串值创建 key，以容纳我们的 HLL 数据结构。
         * 当传入 NULL 时，sdsnewlen() 保证返回的字节已初始化为零。 */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
    } else {
        /* 如果 key 已存在，由于我们在合并不同 HLL 时已经检查过，
         * 因此可以确信它是正确类型/大小，无需再次检查。 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    /* 如果至少一个输入是稠密的，则将目标对象转换为稠密表示。 */
    if (use_dense && hllSparseToDense(o) == C_ERR) {
        addReplyError(c,invalid_hll_err);
        return;
    }

    /* 将结果 HLL 写入目标 HLL 寄存器并使缓存值失效。 */
    for (j = 0; j < HLL_REGISTERS; j++) {
        if (max[j] == 0) continue;
        hdr = o->ptr;
        switch(hdr->encoding) {
        case HLL_DENSE: hllDenseSet(hdr->registers,j,max[j]); break;
        case HLL_SPARSE: hllSparseSet(o,j,max[j]); break;
        }
    }
    hdr = o->ptr; /* o->ptr 现在可能不同了，这是最后一次 hllSparseSet()
                     调用的副作用。 */
    HLL_INVALIDATE_CACHE(hdr);

    signalModifiedKey(c,c->db,c->argv[1]);
    /* 我们为 PFMERGE 生成一个 PFADD 事件以简化语义，
     * 因为从理论上讲这是一次大量元素的添加。 */
    notifyKeyspaceEvent(NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
    server.dirty++;
    addReply(c,shared.ok);
}

/* ========================== Testing / Debugging  ========================== */

/* PFSELFTEST
 * 该命令执行 HLL 寄存器实现的自我测试。
 * 这是从外部难以测试的内容。 */
#define HLL_TEST_CYCLES 1000
void pfselftestCommand(client *c) {
    unsigned int j, i;
    sds bitcounters = sdsnewlen(NULL,HLL_DENSE_SIZE);
    struct hllhdr *hdr = (struct hllhdr*) bitcounters, *hdr2;
    robj *o = NULL;
    uint8_t bytecounters[HLL_REGISTERS];

    /* 测试 1：访问寄存器。
     * 该测试旨在验证数据结构的各个计数器可访问，
     * 并且设置它们的值既能保留正确的值又不会影响相邻的值。 */
    for (j = 0; j < HLL_TEST_CYCLES; j++) {
        /* 将 HLL 计数器和一个同样大小的无符号字节数组
         * 设置为同一组随机值。 */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int r = rand() & HLL_REGISTER_MAX;

            bytecounters[i] = r;
            HLL_DENSE_SET_REGISTER(hdr->registers,i,r);
        }
        /* 检查我们是否能够检索到相同的值。 */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val != bytecounters[i]) {
                addReplyErrorFormat(c,
                    "TESTFAILED Register %d should be %d but is %d",
                    i, (int) bytecounters[i], (int) val);
                goto cleanup;
            }
        }
    }

    /* 测试 2：近似误差。
     * 该测试添加唯一元素并检查估算值是否始终在合理范围内。
     *
     * 我们检查误差是否小于预期标准误差的几倍，使得测试由于"糟糕"的
     * 运行而失败的可能性极小。
     *
     * 该测试同时使用稠密和稀疏 HLL 执行，还验证计算出的基数是否相同。 */
    memset(hdr->registers,0,HLL_DENSE_SIZE-HLL_HDR_SIZE);
    o = createHLLObject();
    double relerr = 1.04/sqrt(HLL_REGISTERS);
    int64_t checkpoint = 1;
    uint64_t seed = (uint64_t)rand() | (uint64_t)rand() << 32;
    uint64_t ele;
    for (j = 1; j <= 10000000; j++) {
        ele = j ^ seed;
        hllDenseAdd(hdr->registers,(unsigned char*)&ele,sizeof(ele));
        hllAdd(o,(unsigned char*)&ele,sizeof(ele));

        /* 确保在较小基数时使用稀疏编码。 */
        if (j == checkpoint && j < server.hll_sparse_max_bytes/2) {
            hdr2 = o->ptr;
            if (hdr2->encoding != HLL_SPARSE) {
                addReplyError(c, "TESTFAILED sparse encoding not used");
                goto cleanup;
            }
        }

        /* 检查稠密和稀疏表示是否一致。 */
        if (j == checkpoint && hllCount(hdr,NULL) != hllCount(o->ptr,NULL)) {
                addReplyError(c, "TESTFAILED dense/sparse disagree");
                goto cleanup;
        }

        /* 检查误差。 */
        if (j == checkpoint) {
            int64_t abserr = checkpoint - (int64_t)hllCount(hdr,NULL);
            uint64_t maxerr = ceil(relerr*6*checkpoint);

            /* 调整我们针对基数为 10 时所期望的最大误差，
             * 因为偶尔由于冲突可能会产生大得多的误差，
             * 导致误报。 */
            if (j == 10) maxerr = 1;

            if (abserr < 0) abserr = -abserr;
            if (abserr > (int64_t)maxerr) {
                addReplyErrorFormat(c,
                    "TESTFAILED Too big error. card:%llu abserr:%llu",
                    (unsigned long long) checkpoint,
                    (unsigned long long) abserr);
                goto cleanup;
            }
            checkpoint *= 10;
        }
    }

    /* 成功！ */
    addReply(c,shared.ok);

cleanup:
    sdsfree(bitcounters);
    if (o) decrRefCount(o);
}

/* 关于 HLL 实现的不同调试相关操作。
 *
 * PFDEBUG GETREG <key>
 * PFDEBUG DECODE <key>
 * PFDEBUG ENCODING <key>
 * PFDEBUG TODENSE <key>
 */
void pfdebugCommand(client *c) {
    char *cmd = c->argv[1]->ptr;
    struct hllhdr *hdr;
    robj *o;
    int j;

    o = lookupKeyWrite(c->db,c->argv[2]);
    if (o == NULL) {
        addReplyError(c,"The specified key does not exist");
        return;
    }
    if (isHLLObjectOrReply(c,o) != C_OK) return;
    o = dbUnshareStringValue(c->db,c->argv[2],o);
    hdr = o->ptr;

    /* PFDEBUG GETREG <key> */
    if (!strcasecmp(cmd,"getreg")) {
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            server.dirty++; /* 在编码变化时强制传播。 */
        }

        hdr = o->ptr;
        addReplyArrayLen(c,HLL_REGISTERS);
        for (j = 0; j < HLL_REGISTERS; j++) {
            uint8_t val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,j);
            addReplyLongLong(c,val);
        }
    }
    /* PFDEBUG DECODE <key> */
    else if (!strcasecmp(cmd,"decode")) {
        if (c->argc != 3) goto arityerr;

        uint8_t *p = o->ptr, *end = p+sdslen(o->ptr);
        sds decoded = sdsempty();

        if (hdr->encoding != HLL_SPARSE) {
            sdsfree(decoded);
            addReplyError(c,"HLL encoding is not sparse");
            return;
        }

        p += HLL_HDR_SIZE;
        while(p < end) {
            int runlen, regval;

            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                p++;
                decoded = sdscatprintf(decoded,"z:%d ",runlen);
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                p += 2;
                decoded = sdscatprintf(decoded,"Z:%d ",runlen);
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                p++;
                decoded = sdscatprintf(decoded,"v:%d,%d ",regval,runlen);
            }
        }
        decoded = sdstrim(decoded," ");
        addReplyBulkCBuffer(c,decoded,sdslen(decoded));
        sdsfree(decoded);
    }
    /* PFDEBUG ENCODING <key> */
    else if (!strcasecmp(cmd,"encoding")) {
        char *encodingstr[2] = {"dense","sparse"};
        if (c->argc != 3) goto arityerr;

        addReplyStatus(c,encodingstr[hdr->encoding]);
    }
    /* PFDEBUG TODENSE <key> */
    else if (!strcasecmp(cmd,"todense")) {
        int conv = 0;
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            conv = 1;
            server.dirty++; /* 在编码变化时强制传播。 */
        }
        addReply(c,conv ? shared.cone : shared.czero);
    } else {
        addReplyErrorFormat(c,"Unknown PFDEBUG subcommand '%s'", cmd);
    }
    return;

arityerr:
    addReplyErrorFormat(c,
        "Wrong number of arguments for the '%s' subcommand",cmd);
}

