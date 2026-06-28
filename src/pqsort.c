/* pqsort.c - 支持局部排序范围的并行快速排序实现
 *
 * 本文件是 NetBSD libc qsort 实现的修改版本, 为 Redis 添加了局部排序支持.
 *
 * 原始版权声明见下方.
 *
 * Copyright(C) 2009-current Redis Ltd.. All rights reserved.
 */


/*  $NetBSD: qsort.c,v 1.19 2009/01/30 23:38:44 lukem Exp $   */

/*-
 * Copyright (c) 1992, 1993
 *      The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <sys/types.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>

static inline char	*med3 (char *, char *, char *,
    int (*)(const void *, const void *));
static inline void	 swapfunc (char *, char *, size_t, int);

#define min(a, b)	(a) < (b) ? a : b

/*
 * Qsort 快速排序核心算法, 源自 Bentley & McIlroy 的
 * "Engineering a Sort Function" 论文实现的 Med-3 切分策略
 *
 * 算法特点:
 * - 使用三数取中(median-of-three)选择枢轴, 避免最坏情况
 * - 对小数组(<7元素)使用插入排序优化
 * - 对大数组使用多级三数取中提高枢轴质量
 * - 支持局部范围排序(lrange, rrange参数)
 */
#define swapcode(TYPE, parmi, parmj, n) { 		\
	size_t i = (n) / sizeof (TYPE); 		\
	TYPE *pi = (TYPE *)(void *)(parmi); 		\
	TYPE *pj = (TYPE *)(void *)(parmj); 		\
	do { 						\
		TYPE	t = *pi;			\
		*pi++ = *pj;				\
		*pj++ = t;				\
        } while (--i > 0);				\
}

#define SWAPINIT(a, es) swaptype = (uintptr_t)a % sizeof(long) || \
	es % sizeof(long) ? 2 : es == sizeof(long)? 0 : 1;

static inline void
swapfunc(char *a, char *b, size_t n, int swaptype)
{

	if (swaptype <= 1)
		swapcode(long, a, b, n)
	else
		swapcode(char, a, b, n)
}

#define swap(a, b)						\
	if (swaptype == 0) {					\
		long t = *(long *)(void *)(a);			\
		*(long *)(void *)(a) = *(long *)(void *)(b);	\
		*(long *)(void *)(b) = t;			\
	} else							\
		swapfunc(a, b, es, swaptype)

#define vecswap(a, b, n) if ((n) > 0) swapfunc((a), (b), (size_t)(n), swaptype)

/* med3 - 三数取中函数, 返回三个数的中位数
 *
 * 参数:
 *   a, b, c - 三个待比较的元素指针
 *   cmp     - 比较函数
 *
 * 返回:
 *   三个元素中比较结果居中的那个元素的指针
 *
 * 作用:
 *   选择一个好的枢轴(pivot)用于快速排序的分区操作,
 *   避免当数组已有序时退化为 O(n^2) 的最坏情况
 */
static inline char *
med3(char *a, char *b, char *c,
    int (*cmp) (const void *, const void *))
{

	return cmp(a, b) < 0 ?
	       (cmp(b, c) < 0 ? b : (cmp(a, c) < 0 ? c : a ))
              :(cmp(b, c) > 0 ? b : (cmp(a, c) < 0 ? a : c ));
}

/* _pqsort - 内部递归快速排序函数
 *
 * 参数:
 *   a      - 待排序数组指针
 *   n      - 数组元素个数
 *   es     - 每个元素的大小(字节)
 *   cmp    - 比较函数
 *   lrange - 局部排序左边界(在此范围内的元素会被排序)
 *   rrange - 局部排序右边界
 *
 * 算法步骤:
 *   1. 小数组(n<7)使用插入排序
 *   2. 选择枢轴: 大数组时使用三数取中策略
 *   3. 分区: 将数组分为小于枢轴、等于枢轴、大于枢轴三部分
 *   4. 递归排序左右两部分(仅在范围内)
 *   5. 迭代而非递归以节省栈空间
 */
static void
_pqsort(void *a, size_t n, size_t es,
    int (*cmp) (const void *, const void *), void *lrange, void *rrange)
{
	char *pa, *pb, *pc, *pd, *pl, *pm, *pn;
	size_t d, r;
	int swaptype, cmp_result;

loop:	SWAPINIT(a, es);
	if (n < 7) {
		for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl) > 0;
			     pl -= es)
				swap(pl, pl - es);
		return;
	}
	pm = (char *) a + (n / 2) * es;
	if (n > 7) {
		pl = (char *) a;
		pn = (char *) a + (n - 1) * es;
		if (n > 40) {
			d = (n / 8) * es;
			pl = med3(pl, pl + d, pl + 2 * d, cmp);
			pm = med3(pm - d, pm, pm + d, cmp);
			pn = med3(pn - 2 * d, pn - d, pn, cmp);
		}
		pm = med3(pl, pm, pn, cmp);
	}
	swap(a, pm);
	pa = pb = (char *) a + es;

	pc = pd = (char *) a + (n - 1) * es;
	for (;;) {
		while (pb <= pc && (cmp_result = cmp(pb, a)) <= 0) {
			if (cmp_result == 0) {
				swap(pa, pb);
				pa += es;
			}
			pb += es;
		}
		while (pb <= pc && (cmp_result = cmp(pc, a)) >= 0) {
			if (cmp_result == 0) {
				swap(pc, pd);
				pd -= es;
			}
			pc -= es;
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		pb += es;
		pc -= es;
	}

	pn = (char *) a + n * es;
	r = min(pa - (char *) a, pb - pa);
	vecswap(a, pb - r, r);
	r = min((size_t)(pd - pc), pn - pd - es);
	vecswap(pb, pn - r, r);
	if ((r = pb - pa) > es) {
                void *_l = a, *_r = ((unsigned char*)a)+r-1;
                if (!((lrange < _l && rrange < _l) ||
                    (lrange > _r && rrange > _r)))
		    _pqsort(a, r / es, es, cmp, lrange, rrange);
        }
	if ((r = pd - pc) > es) {
                void *_l, *_r;

		/* Iterate rather than recurse to save stack space */
		a = pn - r;
		n = r / es;

                _l = a;
                _r = ((unsigned char*)a)+r-1;
                if (!((lrange < _l && rrange < _l) ||
                    (lrange > _r && rrange > _r)))
		    goto loop;
	}
/*		qsort(pn - r, r / es, es, cmp);*/
}

/* pqsort - 局部排序快速排序的对外接口
 *
 * 参数:
 *   a      - 待排序数组指针
 *   n      - 数组元素个数
 *   es     - 每个元素的大小(字节)
 *   cmp    - 比较函数
 *   lrange - 局部排序的起始索引
 *   rrange - 局部排序的结束索引
 *
 * 注意:
 *   仅对 [lrange, rrange] 范围内的元素进行排序,
 *   范围外的元素位置保持不变, 但可能作为排序过程中的临时位置
 */
void
pqsort(void *a, size_t n, size_t es,
    int (*cmp) (const void *, const void *), size_t lrange, size_t rrange)
{
    _pqsort(a,n,es,cmp,((unsigned char*)a)+(lrange*es),
                       ((unsigned char*)a)+((rrange+1)*es)-1);
}
