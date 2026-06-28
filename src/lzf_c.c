/*
 * Copyright (c) 2000-2010 Marc Alexander Lehmann <schmorp@schmorp.de>
 *
 * Redistribution and use in source and binary forms, with or without modifica-
 * tion, are permitted provided that the following conditions are met:
 *
 *   1.  Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *   2.  Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MER-
 * CHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPE-
 * CIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTH-
 * ERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Alternatively, the contents of this file may be used under the terms of
 * the GNU General Public License ("GPL") version 2 or any later version,
 * in which case the provisions of the GPL are applicable instead of
 * the above. If you wish to allow the use of your version of this file
 * only under the terms of the GPL and not to allow others to use your
 * version of this file under the BSD license, indicate your decision
 * by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL. If you do not delete the
 * provisions above, a recipient may use your version of this file under
 * either the BSD or the GPL.
 */

#include "lzfP.h"

#define HSIZE (1 << (HLOG))

/*
 * LZF 压缩算法的核心配置与哈希函数定义
 *
 * 注意：不要随意修改这些参数，除非进行过基准测试！
 * 数据格式并不依赖于哈希函数的具体实现。
 * 哈希函数看起来可能很奇怪，但请相信它确实有效 ;)
 *
 * HLOG: 哈希表大小参数, HSIZE = 2^HLOG
 * FRST: 从前两个字节计算初始哈希值
 * NEXT: 根据新字节更新哈希值
 * IDX: 根据哈希值计算哈希表索引, 有三种速度/压缩率权衡模式
 *   - ULTRA_FAST: 极快速模式, 压缩率略低
 *   - VERY_FAST: 快速模式
 *   - 默认: 平衡模式, 压缩率最佳
 */
#ifndef FRST
# define FRST(p) (((p[0]) << 8) | p[1])
# define NEXT(v,p) (((v) << 8) | p[2])
# if ULTRA_FAST
#  define IDX(h) ((( h             >> (3*8 - HLOG)) - h  ) & (HSIZE - 1))
# elif VERY_FAST
#  define IDX(h) ((( h             >> (3*8 - HLOG)) - h*5) & (HSIZE - 1))
# else
#  define IDX(h) ((((h ^ (h << 5)) >> (3*8 - HLOG)) - h*5) & (HSIZE - 1))
# endif
#endif
/*
 * IDX works because it is very similar to a multiplicative hash, e.g.
 * ((h * 57321 >> (3*8 - HLOG)) & (HSIZE - 1))
 * the latter is also quite fast on newer CPUs, and compresses similarly.
 *
 * the next one is also quite good, albeit slow ;)
 * (int)(cos(h & 0xffffff) * 1e6)
 */

#if 0
/* original lzv-like hash function, much worse and thus slower */
# define FRST(p) (p[0] << 5) ^ p[1]
# define NEXT(v,p) ((v) << 5) ^ p[2]
# define IDX(h) ((h) & (HSIZE - 1))
#endif

#define        MAX_LIT        (1 <<  5)
#define        MAX_OFF        (1 << 13)
#define        MAX_REF        ((1 << 8) + (1 << 3))

#if __GNUC__ >= 3
# define expect(expr,value)         __builtin_expect ((expr),(value))
# define inline                     inline
#else
# define expect(expr,value)         (expr)
# define inline                     static
#endif

#define expect_false(expr) expect ((expr) != 0, 0)
#define expect_true(expr)  expect ((expr) != 0, 1)

#if defined(__has_attribute)
# if __has_attribute(no_sanitize)
#  define NO_SANITIZE(sanitizer) __attribute__((no_sanitize(sanitizer)))
# endif
#endif

#if !defined(NO_SANITIZE)
# define NO_SANITIZE(sanitizer)
#endif

/*
 * LZF 压缩数据格式说明
 *
 * 字面量(Literal)编码: 000LLLLL <L+1>
 *   - L+1 表示字面量长度, 范围 1~33 字节
 *   - 高3位为0表示这是字面量数据
 *
 * 短回引(Backref)编码: LLLooooo oooooooo
 *   - L+1 表示复制长度, 范围 1~7 字节
 *   - o+1 表示回引偏移量, 范围 1~4096
 *   - 高3位不是000也不是111时为短回引格式
 *
 * 长回引(Backref)编码: 111ooooo LLLLLLLL oooooooo
 *   - LLLLLLLL 提供额外的8位长度值, 总长度 = L+8
 *   - o+1 表示回引偏移量, 范围 1~4096
 *   - 高3位为111时为长回引格式
 *
 * 总结: 压缩数据由一系列字面量编码和回引编码交替组成
 *
 */
/* LZF 压缩算法核心函数
 *
 * 参数:
 *   in_data  - 输入数据缓冲区
 *   in_len   - 输入数据长度
 *   out_data - 输出压缩数据缓冲区
 *   out_len  - 输出缓冲区长度
 *   htab     - 哈希表(可选, 由 LZF_STATE_ARG 宏控制)
 *
 * 返回值:
 *   成功返回压缩后数据长度, 失败返回0
 *
 * 算法概述:
 *   1. 使用滑动窗口和哈希表查找重复字节序列
 *   2. 对于重复序列, 使用回引(back-reference)编码: [偏移量, 长度]
 *   3. 对于不可压缩的字节, 使用字面量(literal)编码
 *   4. 哈希表用于快速查找输入数据中是否存在与当前窗口匹配的序列
 */
NO_SANITIZE("alignment")
size_t
lzf_compress (const void *const in_data, size_t in_len,
	      void *out_data, size_t out_len
#if LZF_STATE_ARG
              , LZF_STATE htab
#endif
              )
{
#if !LZF_STATE_ARG
  LZF_STATE htab;
#endif
  const u8 *ip = (const u8 *)in_data;
        u8 *op = (u8 *)out_data;
  const u8 *in_end  = ip + in_len;
        u8 *out_end = op + out_len;
  const u8 *ref;

  /* off requires a type wide enough to hold a general pointer difference.
   * ISO C doesn't have that (size_t might not be enough and ptrdiff_t only
   * works for differences within a single object). We also assume that no
   * no bit pattern traps. Since the only platform that is both non-POSIX
   * and fails to support both assumptions is windows 64 bit, we make a
   * special workaround for it.
   */
#if defined (WIN32) && defined (_M_X64)
  unsigned _int64 off; /* workaround for missing POSIX compliance */
#else
  size_t off;
#endif
  unsigned int hval;
  int lit;

  if (!in_len || !out_len)
    return 0;

#if INIT_HTAB
  /* 初始化哈希表为0 */
  memset (htab, 0, sizeof (htab));
#endif

  /* lit 记录当前字面量序列的长度, op++ 为字面量长度字段预留空间 */
  lit = 0; op++; /* start run */

  /* 计算第一个哈希值(前两个字节) */
  hval = FRST (ip);
  while (ip < in_end - 2)
    {
      LZF_HSLOT *hslot;

      /* 根据新字节更新哈希值 */
      hval = NEXT (hval, ip);
      /* 在哈希表中查找是否有匹配的先前位置 */
      hslot = htab + IDX (hval);
      /* 获取之前记录的位置(ref), 避免对空指针应用零偏移 */
      ref = *hslot ? (*hslot + LZF_HSLOT_BIAS) : NULL;
      /* 将当前位置记录到哈希表 */
      *hslot = ip - LZF_HSLOT_BIAS;

      /* 检查是否找到有效的匹配:
       * 1. ref 指向的位置在当前指针之前
       * 2. 偏移量在有效范围内(MAX_OFF)
       * 3. ref 指向的内存仍在输入数据范围内
       * 4. ref 和 ip 指向的序列前3字节匹配
       * 5. 前2字节也匹配(针对不同对齐方式的检查)
       */
      if (1
#if INIT_HTAB
          && ref < ip /* 下一个测试实际上会处理这种情况, 但这个检查更快 */
#endif
          && (off = ip - ref - 1) < MAX_OFF
          && ref > (u8 *)in_data
          && ref[2] == ip[2]
#if STRICT_ALIGN
          && ((ref[1] << 8) | ref[0]) == ((ip[1] << 8) | ip[0])
#else
          && *(u16 *)ref == *(u16 *)ip
#endif
        )
        {
          /* 找到匹配! 计算匹配长度 */
          unsigned int len = 2; /* 已知至少匹配2字节(ref[0,1] == ip[0,1]) */
          size_t maxlen = in_end - ip - len; /* 剩余可匹配的最大长度 */
          maxlen = maxlen > MAX_REF ? MAX_REF : maxlen; /* 不超过最大引用长度 */

          /* 检查输出缓冲区是否有足够空间(保守快速检查 + 精确检查) */
          if (expect_false (op + 3 + 1 >= out_end)) /* first a faster conservative test */
            if (op - !lit + 3 + 1 >= out_end) /* second the exact but rare test */
              return 0;

          /* 结束当前的字面量序列: 在长度字段位置写入长度值 */
          op [- lit - 1] = lit - 1; /* stop run */
          op -= !lit; /* 如果长度为0则撤销刚才的op++ */

          /* 循环扩展匹配长度, 直到不匹配或达到最大长度限制 */
          for (;;)
            {
              /* 每次处理16字节, 使用展开循环加速 */
              if (expect_true (maxlen > 16))
                {
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;

                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;

                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;

                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                  len++; if (ref [len] != ip [len]) break;
                }

              /* 处理剩余的字节 */
              do
                len++;
              while (len < maxlen && ref[len] == ip[len]);

              break;
            }

          len -= 2; /* len 现在表示的是(匹配字节数 - 1) */
          ip++;

          /* 编码回引: 根据长度选择短格式或长格式
           * 短格式(1字节): 高5位放长度-1, 低8位放偏移量高位
           * 长格式(2字节): 第一字节高5位为7, 第二字节放额外长度值
           */
          if (len < 7)
            {
              /* 短格式: 0b000LLLoo oooooooo */
              *op++ = (off >> 8) + (len << 5);
            }
          else
            {
              /* 长格式: 0b111LLLoo oooooooo LLLLLLLL */
              *op++ = (off >> 8) + (  7 << 5);
              *op++ = len - 7; /* 额外长度值 */
            }

          *op++ = off; /* 偏移量低位 */

          /* 开始新的字面量序列 */
          lit = 0; op++; /* start run */

          ip += len + 1;

          if (expect_false (ip >= in_end - 2))
            break;

#if ULTRA_FAST || VERY_FAST
          --ip;
# if VERY_FAST && !ULTRA_FAST
          --ip;
# endif
          hval = FRST (ip);

          hval = NEXT (hval, ip);
          htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
          ip++;

# if VERY_FAST && !ULTRA_FAST
          hval = NEXT (hval, ip);
          htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
          ip++;
# endif
#else
          ip -= len + 1;

          do
            {
              hval = NEXT (hval, ip);
              htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
              ip++;
            }
          while (len--);
#endif
        }
      else
        {
          /* 没有找到匹配, 将当前字节作为字面量处理 */
          if (expect_false (op >= out_end))
            return 0;

          lit++; *op++ = *ip++; /* 复制字节到输出 */

          /* 字面量序列过长, 需要结束当前序列并开始新的 */
          if (expect_false (lit == MAX_LIT))
            {
              op [- lit - 1] = lit - 1; /* stop run */
              lit = 0; op++; /* start run */
            }
        }
    }

  /* 确保输出缓冲区有足够空间(最多可能缺少3字节) */
  if (op + 3 > out_end) /* at most 3 bytes can be missing here */
    return 0;

  /* 处理最后剩余的字节作为字面量 */
  while (ip < in_end)
    {
      lit++; *op++ = *ip++;

      if (expect_false (lit == MAX_LIT))
        {
          op [- lit - 1] = lit - 1; /* stop run */
          lit = 0; op++; /* start run */
        }
    }

  /* 结束最后一个字面量序列 */
  op [- lit - 1] = lit - 1; /* end run */
  op -= !lit; /* 如果最后一个序列长度为0则撤销 */

  return op - (u8 *)out_data; /* 返回压缩后数据长度 */
}

