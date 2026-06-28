/* Pseudo random number generation functions derived from the drand48()
 * function obtained from pysam source code.
 *
 * This functions are used in order to replace the default math.random()
 * Lua implementation with something having exactly the same behavior
 * across different systems (by default Lua uses libc's rand() that is not
 * required to implement a specific PRNG generating the same sequence
 * in different systems if seeded with the same integer).
 *
 * The original code appears to be under the public domain.
 * I modified it removing the non needed functions and all the
 * 1960-style C coding stuff...
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2010-current, Redis Ltd.
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

#include <stdint.h>

/* N = 16：线性同余生成器的状态位数（48 位） */
#define N	16
/* MASK = 0xFFFF：低 16 位的掩码 */
#define MASK	((1 << (N - 1)) + (1 << (N - 1)) - 1)
/* 取 x 的低 16 位 */
#define LOW(x)	((unsigned)(x) & MASK)
/* 取 x 右移 N 位后的低 16 位 */
#define HIGH(x)	LOW((x) >> N)
/* MUL：计算 x * y，将结果拆分为高低 16 位存入 z */
#define MUL(x, y, z)	{ int32_t l = (long)(x) * (long)(y); \
		(z)[0] = LOW(l); (z)[1] = HIGH(l); }
/* CARRY：检测两个 16 位数相加是否产生进位 */
#define CARRY(x, y)	((int32_t)(x) + (long)(y) > MASK)
/* ADDEQU：将 y 加到 x，更新进位标志 z，返回低 16 位结果 */
#define ADDEQU(x, y, z)	(z = CARRY(x, (y)), x = LOW(x + (y)))
#define X0	0x330E   /* 初始状态分量 0 */
#define X1	0xABCD   /* 初始状态分量 1 */
#define X2	0x1234   /* 初始状态分量 2 */
#define A0	0xE66D   /* 初始乘子分量 0 */
#define A1	0xDEEC   /* 初始乘子分量 1 */
#define A2	0x5      /* 初始乘子分量 2 */
#define C	0xB      /* 初始加法常数 */
#define SET3(x, x0, x1, x2)	((x)[0] = (x0), (x)[1] = (x1), (x)[2] = (x2))
#define SETLOW(x, y, n) SET3(x, LOW((y)[n]), LOW((y)[(n)+1]), LOW((y)[(n)+2]))
#define SEED(x0, x1, x2) (SET3(x, x0, x1, x2), SET3(a, A0, A1, A2), c = C)
#define REST(v)	for (i = 0; i < 3; i++) { xsubi[i] = x[i]; x[i] = temp[i]; } \
		return (v);
/* 符号位：2*N-1 = 31 位处的进位标志 */
#define HI_BIT	(1L << (2 * N - 1))

/* 内部状态：x[3] 为 48 位线性同余状态，a[3] 为乘子，c 为加法常数 */
static uint32_t x[3] = { X0, X1, X2 }, a[3] = { A0, A1, A2 }, c = C;
static void next(void);

/* redisLrand48 - 返回一个 31 位的伪随机整数（正数）
 * 等价于标准 drand48() 的返回值 */
int32_t redisLrand48(void) {
    next();
    return (((int32_t)x[2] << (N - 1)) + (x[1] >> 1));
}

/* redisSrand48 - 使用种子值初始化随机数生成器 */
void redisSrand48(int32_t seedval) {
    SEED(X0, LOW(seedval), HIGH(seedval));
}

/* next - 线性同余生成器（LCG）的核心迭代函数
 * 使用递推公式：X_n+1 = (a * X_n + c) mod m
 * 其中 m = 2^48，a 和 c 为常数 */
static void next(void) {
    uint32_t p[2], q[2], r[2], carry0, carry1;

    MUL(a[0], x[0], p);
    ADDEQU(p[0], c, carry0);
    ADDEQU(p[1], carry0, carry1);
    MUL(a[0], x[1], q);
    ADDEQU(p[1], q[0], carry0);
    MUL(a[1], x[0], r);
    /* 组合多个乘积项得到新的状态 */
    x[2] = LOW(carry0 + carry1 + CARRY(p[1], r[0]) + q[1] + r[1] +
            a[0] * x[2] + a[1] * x[1] + a[2] * x[0]);
    x[1] = LOW(p[1] + r[0]);
    x[0] = LOW(p[0]);
}
