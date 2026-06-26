/*
 * Copyright (c) 2009-current, Redis Ltd.
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * 有序集合（Sorted set）API
 *----------------------------------------------------------------------------*/

/* ZSET（有序集合）使用两种数据结构来保存相同的元素，
 * 以便在有序数据结构中实现 O(log(N)) 的 INSERT 和 REMOVE 操作。
 *
 * 元素会被添加到一个哈希表（Redis 对象 -> 分数）中。
 * 同时元素会被添加到一个跳表（分数 -> Redis 对象），所以
 * 在这个 "视图" 中对象会按照分数排序。
 *
 * 注意：表示元素的 SDS 字符串在哈希表和跳表之间是共享的，以节省内存。
 * 为了更方便地管理共享的 SDS 字符串，我们只在 zslFreeNode() 中释放 SDS。
 * 字典没有设置 value 的释放方法。所以我们总是先从字典中删除元素，
 * 然后再从跳表中删除。
 *
 * 此跳表实现几乎是 William Pugh 在论文 "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees" 中描述的原始算法的 C 语言翻译，
 * 但做了三处修改：
 * a) 此实现允许分数重复。
 * b) 比较不仅仅依据 key（即我们的 'score'），还依据 satellite data。
 * c) 存在后向指针，所以这是一个双向链表，但后向指针只存在于 "level 1"。
 *    这允许从尾部到头部遍历链表，对 ZREVRANGE 很有用。 */

#include "server.h"
#include "intset.h"  /* 紧凑的整数集合结构 */
#include <math.h>

/*-----------------------------------------------------------------------------
 * 跳表（skiplist）底层 API 的实现
 *----------------------------------------------------------------------------*/

int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);
void zsetConvertAndExpand(robj *zobj, int encoding, unsigned long cap);
zskiplistNode *zslGetElementByRankFromNode(zskiplistNode *start_node, int start_level, unsigned long rank);
zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);

/* 创建一个具有指定层数的跳表节点。
 * 调用后，SDS 字符串 'ele' 会被该节点引用。 */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    zskiplistNode *zn =
        zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* 创建一个新的跳表。 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* 释放指定的跳表节点。所引用的元素 SDS 字符串表示也会被释放，
 * 除非在调用此函数前已将 node->ele 设置为 NULL。 */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* 释放整个跳表。 */
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    zfree(zsl->header);
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    zfree(zsl);
}

/* 返回即将创建的跳表新节点的随机层数。
 * 此函数的返回值介于 1 和 ZSKIPLIST_MAXLEVEL 之间（均包含），
 * 服从类似幂律的分布，层数越高出现的概率越低。 */
int zslRandomLevel(void) {
    static const int threshold = ZSKIPLIST_P*RAND_MAX;
    int level = 1;
    while (random() < threshold)
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* 在跳表中插入一个新节点。假定元素尚不存在（由调用者负责强制保证）。
 * 跳表将取得传入的 SDS 字符串 'ele' 的所有权。 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* 存储跨越的 rank 以到达插入位置 */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                    sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* 我们假定元素不在内部，由于允许分数重复，
     * 不应出现重新插入同一元素的情况，因为 zslInsert() 的调用者
     * 应在哈希表中检查元素是否已经在内部。 */
    level = zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }
    x = zslCreateNode(level,score,ele);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* 更新 update[i] 跨越的跨度，因为 x 插入在此处 */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* 增加未触及层的跨度 */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    zsl->length++;
    return x;
}

/* 由 zslDelete、zslDeleteRangeByScore 和 zslDeleteRangeByRank 调用的内部函数。 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

/* 从跳表中删除匹配 score/element 的元素。
 * 如果节点已找到并被删除则返回 1，否则返回 0。
 *
 * 如果 'node' 为 NULL，则被删除的节点由 zslFreeNode() 释放；
 * 否则不会被释放（仅解除链接），并将 *node 设置为该节点指针，
 * 以便调用者可以重用该节点（包括 node->ele 处引用的 SDS 字符串）。 */
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* 可能存在多个具有相同 score 的元素，我们需要找到
     * 同时具有正确 score 和对象的元素。 */
    x = x->level[0].forward;
    if (x && score == x->score && sdscmp(x->ele,ele) == 0) {
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* 未找到 */
}

/* 更新有序集合跳表内某元素的分数。
 * 注意：元素必须存在并且必须匹配 'score'。
 * 此函数不会更新哈希表一端的分数，调用者应自行处理。
 *
 * 注意：此函数会尝试直接更新节点，以防分数更新后该节点恰好处于相同位置。
 * 否则将通过移除并重新添加新元素来修改跳表，这样开销更大。
 *
 * 函数返回更新后的元素跳表节点指针。 */
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* 我们需要从开始处查找要更新的元素：这无论如何都是有用的，
     * 因为我们必须更新或移除它。 */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < curscore ||
                    (x->level[i].forward->score == curscore &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* 跳转到我们的元素：注意此函数假定具有匹配分数的元素存在。 */
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele,ele) == 0);

    /* 如果分数更新后该节点仍恰好处于相同位置，
     * 我们可以直接更新分数而无需真正地从跳表中移除并重新插入元素。 */
    if ((x->backward == NULL || x->backward->score < newscore) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore))
    {
        x->score = newscore;
        return x;
    }

    /* 无法重用旧节点：我们需要移除并在不同的位置插入一个新节点。 */
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl,newscore,x->ele);
    /* 我们复用了旧节点 x->ele 的 SDS 字符串，现在释放该节点，
     * 因为 zslInsert 创建了一个新的。 */
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* 如果 zset 的某部分在范围内则返回 1。 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* 测试总是为空的范围。 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}

/* 查找指定范围内第 N 个节点。N 从 0 开始计数。
 * 负数 N 表示反向顺序（-1 代表最后一个元素）。
 * 当范围内没有元素时返回 NULL。 */
zskiplistNode *zslNthInRange(zskiplist *zsl, zrangespec *range, long n) {
    zskiplistNode *x;
    int i;
    long edge_rank = 0;
    long last_highest_level_rank = 0;
    zskiplistNode *last_highest_level_node = NULL;
    unsigned long rank_diff;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zslIsInRange(zsl,range)) return NULL;

    /* 在 zsl->level-1 层上向前移动直到 *OUT* of range。 */
    x = zsl->header;
    i = zsl->level - 1;
    while (x->level[i].forward && !zslValueGteMin(x->level[i].forward->score, range)) {
        edge_rank += x->level[i].span;
        x = x->level[i].forward;
    }
    /* 记住具有 zsl->level-1 层的最后一个节点及其 rank。 */
    last_highest_level_node = x;
    last_highest_level_rank = edge_rank;

    if (n >= 0) {
        for (i = zsl->level - 2; i >= 0; i--) {
            /* 当 *OUT* of range 时向前移动。 */
            while (x->level[i].forward && !zslValueGteMin(x->level[i].forward->score, range)) {
                /* 统计小于范围的最后一个元素的 rank。 */
                edge_rank += x->level[i].span;
                x = x->level[i].forward;
            }
        }
        /* 检查 zsl 是否足够长。 */
        if ((unsigned long)(edge_rank + n) >= zsl->length) return NULL;
        if (n < ZSKIPLIST_MAX_SEARCH) {
            /* 如果偏移量较小，我们可以逐节点跳转 */
            /* rank+1 是范围内的第一个元素，因此我们需要 n+1 步到达目标。 */
            for (i = 0; i < n + 1; i++) {
                x = x->level[0].forward;
            }
        } else {
            /* 如果偏移量较大，我们可以从最后一个 zsl->level-1 节点跳转。 */
            rank_diff = edge_rank + 1 + n - last_highest_level_rank;
            x = zslGetElementByRankFromNode(last_highest_level_node, zsl->level - 1, rank_diff);
        }
        /* 检查 score 是否 <= max。 */
        if (x && !zslValueLteMax(x->score,range)) return NULL;
    } else  {
        for (i = zsl->level - 1; i >= 0; i--) {
            /* 当 *IN* of range 时向前移动。 */
            while (x->level[i].forward && zslValueLteMax(x->level[i].forward->score, range)) {
                /* 统计范围内最后一个元素的 rank。 */
                edge_rank += x->level[i].span;
                x = x->level[i].forward;
            }
        }
        /* 检查范围是否足够大。 */
        if (edge_rank < -n) return NULL;
        if (n + 1 > -ZSKIPLIST_MAX_SEARCH) {
            /* 如果偏移量较小，我们可以逐节点跳转 */
            /* rank 是范围内的第 -1 个元素，因此我们需要 -n-1 步到达目标。 */
            for (i = 0; i < -n - 1; i++) {
                x = x->backward;
            }
        } else {
            /* 如果偏移量较大，我们可以从最后一个 zsl->level-1 节点跳转。 */
            /* rank 是范围内的最后一个元素，n 是基于 -1 的，
             * 因此我们需要 n+1 来反向计数。 */
            rank_diff = edge_rank + 1 + n - last_highest_level_rank;
            x = zslGetElementByRankFromNode(last_highest_level_node, zsl->level - 1, rank_diff);
        }
        /* 检查 score 是否 >= min。 */
        if (x && !zslValueGteMin(x->score, range)) return NULL;
    }

    return x;
}

/* 删除跳表中分数介于 min 和 max 之间的所有元素。
 * min 和 max 都可以是包含或排除的（参见 range->minex 和 range->maxex）。
 * 当为包含时，会删除 score >= min && score <= max 的元素。
 * 注意此函数会接收对有序集合哈希表视图的引用，
 * 以便也从哈希表中移除元素。 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score, range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* 当前节点是最后一个分数 <= min 的节点。 */
    x = x->level[0].forward;

    /* 在范围内时删除节点。 */
    while (x && zslValueLteMax(x->score, range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x); /* 此处实际释放 x->ele。 */
        removed++;
        x = next;
    }
    return removed;
}

unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* 当前节点是最后一个分数 <= min 的节点。 */
    x = x->level[0].forward;

    /* 在范围内时删除节点。 */
    while (x && zslLexValueLteMax(x->ele,range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x); /* 此处实际释放 x->ele。 */
        removed++;
        x = next;
    }
    return removed;
}

/* 删除跳表中 rank 介于 start 和 end 之间的所有元素。
 * start 和 end 均为闭区间。注意 start 和 end 需要是 1-based。 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* 根据 score 和 key 查找元素的 rank。
 * 如果找不到元素则返回 0，否则返回 rank。
 * 注意由于 zsl->header 到第一个元素的跨度，rank 是 1-based 的。 */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x 可能等于 zsl->header，因此测试 obj 是否为非 NULL */
        if (x->ele && x->score == score && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* 从起始节点按 rank 查找元素。rank 参数必须是 1-based。 */
zskiplistNode *zslGetElementByRankFromNode(zskiplistNode *start_node, int start_level, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = start_node;
    for (i = start_level; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* 按 rank 查找元素。rank 参数必须是 1-based。 */
zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    return zslGetElementByRankFromNode(zsl->header, zsl->level - 1, rank);
}

/* 根据对象 min 和 max 填充 rangespec。 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* 解析 min-max 区间。如果某个值以 "(" 字符为前缀，
     * 则被视为 "开区间"。例如
     * ZRANGEBYSCORE zset (1.5 (2.5  将匹配 min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5   将匹配 min <= x <= max */
    if (min->encoding == OBJ_ENCODING_INT) {
        spec->min = (long)min->ptr;
    } else {
        if (((char*)min->ptr)[0] == '(') {
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long)max->ptr;
    } else {
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ 字典序范围 ---------------------------- */

/* 解析 ZRANGEBYLEX 的 max 或 min 参数。
  * (foo 表示 foo （开区间）
  * [foo 表示 foo （闭区间）
  * - 表示可能的最小字符串
  * + 表示可能的最大字符串
  *
  * 如果字符串有效，则 *dest 指针设置为将用于比较的 redis 对象，
  * 而 ex 将根据该项是排除还是包含分别设置为 0 或 1。返回 C_OK。
  *
  * 如果字符串不是有效的范围，则返回 C_ERR，并且 *dest 和 *ex 的值未定义。 */
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.maxstring;
        return C_OK;
    case '-':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.minstring;
        return C_OK;
    case '(':
        *ex = 1;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    case '[':
        *ex = 0;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    default:
        return C_ERR;
    }
}

/* 释放 lex range 结构，必须在 zslParseLexRange() 成功填充结构（C_OK 返回）之后调用。 */
void zslFreeLexRange(zlexrangespec *spec) {
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring) sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring) sdsfree(spec->max);
}

/* 根据对象 min 和 max 填充 lex rangespec。
 *
 * 成功返回 C_OK，出错则返回 C_ERR。
 * 当返回 OK 时必须使用 zslFreeLexRange() 释放结构，否则无需释放。 */
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* 如果对象是整数编码，则范围不可能有效。
     * 每个项必须以 ( 或 [ 开头。 */
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT) return C_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* 这只是 sdscmp() 的一个包装，
 * 它能够处理 shared.minstring 和 shared.maxstring，
 * 将它们视为字符串的 -inf 和 +inf 等价物。 */
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a,b);
}

int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
        (sdscmplex(value,spec->min) > 0) :
        (sdscmplex(value,spec->min) >= 0);
}

int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
        (sdscmplex(value,spec->max) < 0) :
        (sdscmplex(value,spec->max) <= 0);
}

/* 如果 zset 的某部分在 lex 范围内则返回 1。 */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* 测试总是为空的范围。 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele,range))
        return 0;
    return 1;
}

/* 查找指定范围内第 N 个节点。N 从 0 开始计数。
 * 负数 N 表示反向顺序（-1 代表最后一个元素）。
 * 当范围内没有元素时返回 NULL。 */
zskiplistNode *zslNthInLexRange(zskiplist *zsl, zlexrangespec *range, long n) {
    zskiplistNode *x;
    int i;
    long edge_rank = 0;
    long last_highest_level_rank = 0;
    zskiplistNode *last_highest_level_node = NULL;
    unsigned long rank_diff;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zslIsInLexRange(zsl,range)) return NULL;

    /* 在 zsl->level-1 层上向前移动直到 *OUT* of range。 */
    x = zsl->header;
    i = zsl->level - 1;
    while (x->level[i].forward && !zslLexValueGteMin(x->level[i].forward->ele, range)) {
        edge_rank += x->level[i].span;
        x = x->level[i].forward;
    }
    /* 记住具有 zsl->level-1 层的最后一个节点及其 rank。 */
    last_highest_level_node = x;
    last_highest_level_rank = edge_rank;

    if (n >= 0) {
        for (i = zsl->level - 2; i >= 0; i--) {
            /* 当 *OUT* of range 时向前移动。 */
            while (x->level[i].forward && !zslLexValueGteMin(x->level[i].forward->ele, range)) {
                /* 统计小于范围的最后一个元素的 rank。 */
                edge_rank += x->level[i].span;
                x = x->level[i].forward;
            }
        }
        /* 检查 zsl 是否足够长。 */
        if ((unsigned long)(edge_rank + n) >= zsl->length) return NULL;
        if (n < ZSKIPLIST_MAX_SEARCH) {
            /* 如果偏移量较小，我们可以逐节点跳转 */
            /* rank+1 是范围内的第一个元素，因此我们需要 n+1 步到达目标。 */
            for (i = 0; i < n + 1; i++) {
                x = x->level[0].forward;
            }
        } else {
            /* 如果偏移量较大，我们可以从最后一个 zsl->level-1 节点跳转。 */
            rank_diff = edge_rank + 1 + n - last_highest_level_rank;
            x = zslGetElementByRankFromNode(last_highest_level_node, zsl->level - 1, rank_diff);
        }
        /* 检查 score 是否 <= max。 */
        if (x && !zslLexValueLteMax(x->ele,range)) return NULL;
    } else {
        for (i = zsl->level - 1; i >= 0; i--) {
            /* 当 *IN* of range 时向前移动。 */
            while (x->level[i].forward && zslLexValueLteMax(x->level[i].forward->ele, range)) {
                /* 统计范围内最后一个元素的 rank。 */
                edge_rank += x->level[i].span;
                x = x->level[i].forward;
            }
        }
        /* 检查范围是否足够大。 */
        if (edge_rank < -n) return NULL;
        if (n + 1 > -ZSKIPLIST_MAX_SEARCH) {
            /* 如果偏移量较小，我们可以逐节点跳转 */
            for (i = 0; i < -n - 1; i++) {
                x = x->backward;
            }
        } else {
            /* 如果偏移量较大，我们可以从最后一个 zsl->level-1 节点跳转。 */
            /* rank 是范围内的最后一个元素，n 是基于 -1 的，
             * 因此我们需要 n+1 来反向计数。 */
            rank_diff = edge_rank + 1 + n - last_highest_level_rank;
            x = zslGetElementByRankFromNode(last_highest_level_node, zsl->level - 1, rank_diff);
        }
        /* 检查 score 是否 >= min。 */
        if (x && !zslLexValueGteMin(x->ele, range)) return NULL;
    }

    return x;
}

/*-----------------------------------------------------------------------------
 * 基于 listpack 的有序集合 API
 *----------------------------------------------------------------------------*/

double zzlStrtod(unsigned char *vstr, unsigned int vlen) {
    char buf[128];
    if (vlen > sizeof(buf) - 1)
        vlen = sizeof(buf) - 1;
    memcpy(buf,vstr,vlen);
    buf[vlen] = '\0';
    return strtod(buf,NULL);
 }

double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    double score;

    serverAssert(sptr != NULL);
    vstr = lpGetValue(sptr,&vlen,&vlong);

    if (vstr) {
        score = zzlStrtod(vstr,vlen);
    } else {
        score = vlong;
    }

    return score;
}

/* 将 listpack 元素作为 SDS 字符串返回。 */
sds lpGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    vstr = lpGetValue(sptr,&vlen,&vlong);

    if (vstr) {
        return sdsnewlen((char*)vstr,vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* 比较有序集合中的元素与给定元素。 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    vstr = lpGetValue(eptr,&vlen,&vlong);
    if (vstr == NULL) {
        /* 将 long long 的字符串表示存储在 buf 中。 */
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

unsigned int zzlLength(unsigned char *zl) {
    return lpLength(zl)/2;
}

/* 根据 eptr 和 sptr 中的值移动到下一个条目。
 * 当没有下一个条目时，两者都设置为 NULL。 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    _eptr = lpNext(zl,*sptr);
    if (_eptr != NULL) {
        _sptr = lpNext(zl,_eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* 没有下一个条目。 */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* 根据 eptr 和 sptr 中的值移动到上一个条目。
 * 当没有上一个条目时，两者都设置为 NULL。 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    _sptr = lpPrev(zl,*eptr);
    if (_sptr != NULL) {
        _eptr = lpPrev(zl,_sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* 没有上一个条目。 */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* 如果 zset 的某部分在范围内则返回 1。
 * 仅供 zzlFirstInRange 和 zzlLastInRange 内部使用。 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* 测试总是为空的范围。 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    p = lpSeek(zl,-1); /* 最后一个 score。 */
    if (p == NULL) return 0; /* 空的有序集合 */
    score = zzlGetScore(p);
    if (!zslValueGteMin(score,range))
        return 0;

    p = lpSeek(zl,1); /* 第一个 score。 */
    serverAssert(p != NULL);
    score = zzlGetScore(p);
    if (!zslValueLteMax(score,range))
        return 0;

    return 1;
}

/* 查找指定范围内第一个元素的指针。
 * 当范围内没有元素时返回 NULL。 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;
    double score;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zzlIsInRange(zl,range)) return NULL;

    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);
        if (zslValueGteMin(score,range)) {
            /* 检查 score 是否 <= max。 */
            if (zslValueLteMax(score,range))
                return eptr;
            return NULL;
        }

        /* 移动到下一个元素。 */
        eptr = lpNext(zl,sptr);
    }

    return NULL;
}

/* 查找指定范围内最后一个元素的指针。
 * 当范围内没有元素时返回 NULL。 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = lpSeek(zl,-2), *sptr;
    double score;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zzlIsInRange(zl,range)) return NULL;

    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* 检查 score 是否 >= min。 */
            if (zslValueGteMin(score,range))
                return eptr;
            return NULL;
        }

        /* 通过移动到上一个元素的 score 来移动到上一个元素。
         * 当此返回 NULL 时，我们知道也没有元素。 */
        sptr = lpPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = lpPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    sds value = lpGetObject(p);
    int res = zslLexValueGteMin(value,spec);
    sdsfree(value);
    return res;
}

int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    sds value = lpGetObject(p);
    int res = zslLexValueLteMax(value,spec);
    sdsfree(value);
    return res;
}

/* 如果 zset 的某部分在范围内则返回 1。
 * 仅供 zzlFirstInLexRange 和 zzlLastInLexRange 内部使用。 */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* 测试总是为空的范围。 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    p = lpSeek(zl,-2); /* 最后一个元素。 */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    p = lpSeek(zl,0); /* 第一个元素。 */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    return 1;
}

/* 查找指定 lex 范围内第一个元素的指针。
 * 当范围内没有元素时返回 NULL。 */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr,range)) {
            /* 检查 score 是否 <= max。 */
            if (zzlLexValueLteMax(eptr,range))
                return eptr;
            return NULL;
        }

        /* 移动到下一个元素。 */
        sptr = lpNext(zl,eptr); /* 此元素的 score。跳过它。 */
        serverAssert(sptr != NULL);
        eptr = lpNext(zl,sptr); /* 下一个元素。 */
    }

    return NULL;
}

/* 查找指定 lex 范围内最后一个元素的指针。
 * 当范围内没有元素时返回 NULL。 */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = lpSeek(zl,-2), *sptr;

    /* 如果所有内容都超出范围，则提前返回。 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* 检查 score 是否 >= min。 */
            if (zzlLexValueGteMin(eptr,range))
                return eptr;
            return NULL;
        }

        /* 通过移动到上一个元素的 score 来移动到上一个元素。
         * 当此返回 NULL 时，我们知道也没有元素。 */
        sptr = lpPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = lpPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

unsigned char *zzlFind(unsigned char *lp, sds ele, double *score) {
    unsigned char *eptr, *sptr;

    if ((eptr = lpFirst(lp)) == NULL) return NULL;
    eptr = lpFind(lp, eptr, (unsigned char*)ele, sdslen(ele), 1);
    if (eptr) {
        sptr = lpNext(lp,eptr);
        serverAssert(sptr != NULL);

        /* 匹配的元素，提取 score。 */
        if (score != NULL) *score = zzlGetScore(sptr);
        return eptr;
    }

    return NULL;
}

/* 从 listpack 中删除 (element,score) 对。使用 eptr 的本地副本，
 * 因为我们不想修改作为参数传入的那个。 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    return lpDeleteRangeWithEntry(zl,&eptr,2);
}

unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[MAX_D2STRING_CHARS];
    int scorelen = 0;
    long long lscore;
    int score_is_long = double2ll(score, &lscore);
    if (!score_is_long)
        scorelen = d2string(scorebuf,sizeof(scorebuf),score);
    if (eptr == NULL) {
        zl = lpAppend(zl,(unsigned char*)ele,sdslen(ele));
        if (score_is_long)
            zl = lpAppendInteger(zl,lscore);
        else
            zl = lpAppend(zl,(unsigned char*)scorebuf,scorelen);
    } else {
        /* 在元素 'eptr' 之前插入成员。 */
        zl = lpInsertString(zl,(unsigned char*)ele,sdslen(ele),eptr,LP_BEFORE,&sptr);

        /* 在成员之后插入 score。 */
        if (score_is_long)
            zl = lpInsertInteger(zl,lscore,sptr,LP_AFTER,NULL);
        else
            zl = lpInsertString(zl,(unsigned char*)scorebuf,scorelen,sptr,LP_AFTER,NULL);
    }
    return zl;
}

/* 在 listpack 中插入 (element,score) 对。此函数假定元素尚未存在于列表中。 */
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;
    double s;

    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);
        s = zzlGetScore(sptr);

        if (s > score) {
            /* 第一个 score 大于待插入元素 score 的元素。
             * 这意味着我们应在其位置插入以保持顺序。 */
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* 确保元素的字典序。 */
            if (zzlCompareElements(eptr,(unsigned char*)ele,sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* 移动到下一个元素。 */
        eptr = lpNext(zl,sptr);
    }

    /* 尚未插入时推入列表尾部。 */
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);
    return zl;
}

unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* 当 listpack 的尾部被删除时，eptr 将为 NULL。 */
    while (eptr && (sptr = lpNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* 同时删除元素和 score。 */
            zl = lpDeleteRangeWithEntry(zl,&eptr,2);
            num++;
        } else {
            /* 不再在范围内。 */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInLexRange(zl,range);
    if (eptr == NULL) return zl;

    /* 当 listpack 的尾部被删除时，eptr 将为 NULL。 */
    while (eptr && (sptr = lpNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* 同时删除元素和 score。 */
            zl = lpDeleteRangeWithEntry(zl,&eptr,2);
            num++;
        } else {
            /* 不再在范围内。 */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* 删除 listpack 中 rank 介于 start 和 end 之间的所有元素。
 * start 和 end 均为闭区间。注意 start 和 end 需要是 1-based。 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    zl = lpDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * 通用有序集合 API
 *----------------------------------------------------------------------------*/

unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        length = zzlLength(zobj->ptr);
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        length = ((const zset*)zobj->ptr)->zsl->length;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

/* 返回 zset 的工厂方法。
 *
 * size_hint 表示大约将添加多少项，
 * val_len_hint 表示添加元素的近似单个大小，
 * 它们用于确定初始表示形式。
 *
 * 如果不知道提示值，低估或 0 是合适的。
 * 我们永远不应传递负值，因为它会转换为一个非常大的无符号数。 */
robj *zsetTypeCreate(size_t size_hint, size_t val_len_hint) {
    if (size_hint <= server.zset_max_listpack_entries &&
        val_len_hint <= server.zset_max_listpack_value)
    {
        return createZsetListpackObject();
    }

    robj *zobj = createZsetObject();
    zset *zs = zobj->ptr;
    dictExpand(zs->dict, size_hint);
    return zobj;
}

/* 根据 size hint 检查现有 zset 是否应转换为另一种编码。 */
void zsetTypeMaybeConvert(robj *zobj, size_t size_hint) {
    if (zobj->encoding == OBJ_ENCODING_LISTPACK &&
        size_hint > server.zset_max_listpack_entries)
    {
        zsetConvertAndExpand(zobj, OBJ_ENCODING_SKIPLIST, size_hint);
    }
}

/* 将 zset 转换为指定的编码。（转换为跳表时）zset dict 会被预设大小，
 * 以容纳原始 zset 中的元素数量。 */
void zsetConvert(robj *zobj, int encoding) {
    zsetConvertAndExpand(zobj, encoding, zsetLength(zobj));
}

/* 将 zset 转换为指定的编码，预先为其分配 'cap' 个元素的大小。 */
void zsetConvertAndExpand(robj *zobj, int encoding, unsigned long cap) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    if (zobj->encoding == encoding) return;
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType);
        zs->zsl = zslCreate();

        /* 预设 dict 大小以避免 rehash */
        dictExpand(zs->dict, cap);

        eptr = lpSeek(zl,0);
        if (eptr != NULL) {
            sptr = lpNext(zl,eptr);
            serverAssertWithInfo(NULL,zobj,sptr != NULL);
        }

        while (eptr != NULL) {
            score = zzlGetScore(sptr);
            vstr = lpGetValue(eptr,&vlen,&vlong);
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char*)vstr,vlen);

            node = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            zzlNext(zl,&eptr,&sptr);
        }

        zfree(zobj->ptr);
        zobj->ptr = zs;
        zobj->encoding = OBJ_ENCODING_SKIPLIST;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        unsigned char *zl = lpNew(0);

        if (encoding != OBJ_ENCODING_LISTPACK)
            serverPanic("Unknown target encoding");

        /* 与 zslFree() 类似的方法，因为我们想在创建 listpack 的同时释放跳表。 */
        zs = zobj->ptr;
        dictRelease(zs->dict);
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        while (node) {
            zl = zzlInsertAt(zl,NULL,node->ele,node->score);
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = OBJ_ENCODING_LISTPACK;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* 如果有序集合对象还不是 listpack，并且元素数量、最大元素大小和总元素大小
 * 都在预期范围内，则将其转换为 listpack。 */
void zsetConvertToListpackIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen) {
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_listpack_entries &&
        maxelelen <= server.zset_max_listpack_value &&
        lpSafeToAdd(NULL, totelelen))
    {
        zsetConvert(zobj,OBJ_ENCODING_LISTPACK);
    }
}

/* （通过引用）返回有序集合指定成员的 score，存储到 *score 中。
 * 如果元素不存在，则返回 C_ERR；否则返回 C_OK，并且 *score 被正确填充。
 * 如果 'zobj' 或 'member' 为 NULL，则返回 C_ERR。 */
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de = dictFind(zs->dict, member);
        if (de == NULL) return C_ERR;
        *score = *(double*)dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* 在有序集合中添加新元素或更新现有元素的 score，与其编码无关。
 *
 * flags 集合会改变命令行为。
 *
 * 输入的 flags 如下：
 *
 * ZADD_INCR: 将当前元素的 score 增加 'score'，而不是更新当前元素的 score。
 *            如果元素不存在，我们假定先前的 score 为 0。
 * ZADD_NX:   仅当元素不存在时执行操作。
 * ZADD_XX:   仅当元素已存在时执行操作。
 * ZADD_GT:   仅当新 score 大于当前 score 时，才对现有元素执行操作。
 * ZADD_LT:   仅当新 score 小于当前 score 时，才对现有元素执行操作。
 *
 * 当使用 ZADD_INCR 时，元素的新 score 将存储在 '*newscore' 中（如果 'newscore' 不为 NULL）。
 *
 * 返回的 flags 如下：
 *
 * ZADD_NAN:     结果 score 不是数字。
 * ZADD_ADDED:   元素已添加（调用前不存在）。
 * ZADD_UPDATED: 元素的 score 已更新。
 * ZADD_NOP:     由于 NX 或 XX，未执行任何操作。
 *
 * 返回值：
 *
 * 函数成功时返回 1，并设置适当的 flags ADDED 或 UPDATED 以表示操作期间发生了什么
 * （请注意，如果我们使用元素原先具有的相同 score 重新添加元素，
 * 或者使用了零增量，则可能都不设置）。
 *
 * 函数出错时返回 0，目前仅在增量产生 NAN 条件时，或 'score' 值从一开始就是 NAN 时。
 *
 * 该命令作为添加新元素的副作用，可能会将有序集合内部编码从 listpack 转换为 hashtable+skiplist。
 *
 * 'ele' 的内存管理：
 *
 * 此函数不会取得 'ele' SDS 字符串的所有权，但如果需要会复制它。 */
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore) {
    /* 将选项转换为易于检查的变量。 */
    int incr = (in_flags & ZADD_IN_INCR) != 0;
    int nx = (in_flags & ZADD_IN_NX) != 0;
    int xx = (in_flags & ZADD_IN_XX) != 0;
    int gt = (in_flags & ZADD_IN_GT) != 0;
    int lt = (in_flags & ZADD_IN_LT) != 0;
    *out_flags = 0; /* 我们将返回响应 flags。 */
    double curscore;

    /* NaN 作为输入是错误，无论其他参数如何。 */
    if (isnan(score)) {
        *out_flags = ZADD_OUT_NAN;
        return 0;
    }

    /* 根据编码更新有序集合。 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *eptr;

        if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
            /* NX？直接返回，相同元素已存在。 */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            /* 如果需要，为增量准备 score。 */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT？仅当 score 大于/小于当前值时才更新。 */
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* 当 score 改变时，移除并重新插入。 */
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;
        } else if (!xx) {
            /* 在执行 zzlInsert 之前检查元素是否过大或列表变得过长。 */
            if (zzlLength(zobj->ptr)+1 > server.zset_max_listpack_entries ||
                sdslen(ele) > server.zset_max_listpack_value ||
                !lpSafeToAdd(zobj->ptr, sdslen(ele)))
            {
                zsetConvertAndExpand(zobj, OBJ_ENCODING_SKIPLIST, zsetLength(zobj) + 1);
            } else {
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                if (newscore) *newscore = score;
                *out_flags |= ZADD_OUT_ADDED;
                return 1;
            }
        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    }

    /* 注意上面的 listpack 处理块要么已返回，要么已将 key 转换为 skiplist。 */
    if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            /* NX？直接返回，相同元素已存在。 */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            curscore = *(double*)dictGetVal(de);

            /* 如果需要，为增量准备 score。 */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT？仅当 score 大于/小于当前值时才更新。 */
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* 当 score 改变时，移除并重新插入。 */
            if (score != curscore) {
                znode = zslUpdateScore(zs->zsl,curscore,ele,score);
                /* 注意我们没有从表示有序集合的哈希表中移除原始元素，
                 * 因此我们仅更新 score。 */
                dictSetVal(zs->dict, de, &znode->score); /* 更新 score 指针。 */
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;
        } else if (!xx) {
            ele = sdsdup(ele);
            znode = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
            *out_flags |= ZADD_OUT_ADDED;
            if (newscore) *newscore = score;
            return 1;
        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* 不会到达。 */
}

/* 从编码为 skiplist+dict 的有序集合中删除元素 'ele'，
 * 如果元素存在并被删除则返回 1，否则返回 0（元素不存在）。
 * 它不会在删除元素后调整 dict 的大小。 */
static int zsetRemoveFromSkiplist(zset *zs, sds ele) {
    dictEntry *de;
    double score;

    de = dictUnlink(zs->dict,ele);
    if (de != NULL) {
        /* 获取 score 以便稍后从跳表中删除。 */
        score = *(double*)dictGetVal(de);

        /* 从哈希表中删除，然后从跳表中删除。
         * 注意顺序很重要：从跳表中删除实际上会释放表示元素的 SDS 字符串，
         * 它在跳表和哈希表之间是共享的，因此我们需要将
         * 从跳表中删除作为最后一步。 */
        dictFreeUnlinkedEntry(zs->dict,de);

        /* 从跳表中删除。 */
        int retval = zslDelete(zs->zsl,score,ele,NULL);
        serverAssert(retval);

        return 1;
    }

    return 0;
}

/* 从有序集合中删除元素 'ele'，如果元素存在并被删除则返回 1，
 * 否则返回 0（元素不存在）。 */
int zsetDel(robj *zobj, sds ele) {
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *eptr;

        if ((eptr = zzlFind(zobj->ptr,ele,NULL)) != NULL) {
            zobj->ptr = zzlDelete(zobj->ptr,eptr);
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        if (zsetRemoveFromSkiplist(zs, ele)) {
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* 未找到该元素。 */
}

/* 给定一个有序集合对象，返回该对象的 0-based rank，
 * 如果对象不存在则返回 -1。
 *
 * rank 指的是元素在有序集合中的位置。因此第一个元素的 rank 为 0，
 * 第二个元素的 rank 为 1，依此类推，直到 length-1 个元素。
 *
 * 如果 'reverse' 为 false，则按分数最低的元素作为第一个元素返回 rank。
 * 否则，如果 'reverse' 非零，则按分数最高的元素作为 rank 为 0 的元素计算 rank。 */
long zsetRank(robj *zobj, sds ele, int reverse, double *output_score) {
    unsigned long llen;
    unsigned long rank;

    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = lpSeek(zl,0);
        serverAssert(eptr != NULL);
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        rank = 1;
        while(eptr != NULL) {
            if (lpCompare(eptr,(unsigned char*)ele,sdslen(ele)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            if (output_score)
                *output_score = zzlGetScore(sptr);
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            rank = zslGetRank(zsl,score,ele);
            /* 已存在的元素总是有 rank。 */
            serverAssert(rank != 0);
            if (output_score)
                *output_score = score;
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* 这是 COPY 命令的辅助函数。
 * 复制一个有序集合对象，保证返回的对象与原对象具有相同的编码。
 *
 * 生成的对象的 refcount 始终设置为 1 */
robj *zsetDup(robj *o) {
    robj *zobj;
    zset *zs;
    zset *new_zs;

    serverAssert(o->type == OBJ_ZSET);

    /* 创建一个新的有序集合对象，其编码与原对象的编码相同 */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        zobj = createObject(OBJ_ZSET, new_zl);
        zobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zobj = createZsetObject();
        zs = o->ptr;
        new_zs = zobj->ptr;
        dictExpand(new_zs->dict,dictSize(zs->dict));
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;
        long llen = zsetLength(o);

        /* 我们从最大到最小复制跳表元素（这很简单，因为元素已经在跳表中排序）：
         * 这可以改善加载过程，因为下一个加载的元素总是较小的，所以添加到跳表
         * 总是立即在头部停止，使插入复杂度为 O(1) 而非 O(log(N))。 */
        ln = zsl->tail;
        while (llen--) {
            ele = ln->ele;
            sds new_ele = sdsdup(ele);
            zskiplistNode *znode = zslInsert(new_zs->zsl,ln->score,new_ele);
            dictAdd(new_zs->dict,new_ele,&znode->score);
            ln = ln->backward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return zobj;
}

/* 从 listpack 条目创建一个新的 sds 字符串。 */
sds zsetSdsFromListpackEntry(listpackEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* 通过 bulk string 从 listpack 条目进行回复。 */
void zsetReplyFromListpackEntry(client *c, listpackEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}


/* 从非空 zset 返回随机元素。
 * 'key' 和 'val' 将被设置为持有该元素。
 * `key` 中的内存不应由调用者释放或修改。
 * 'score' 可以为 NULL，此时不会提取它。 */
void zsetTypeRandomElement(robj *zsetobj, unsigned long zsetsize, listpackEntry *key, double *score) {
    if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zsetobj->ptr;
        dictEntry *de = dictGetFairRandomKey(zs->dict);
        sds s = dictGetKey(de);
        key->sval = (unsigned char*)s;
        key->slen = sdslen(s);
        if (score)
            *score = *(double*)dictGetVal(de);
    } else if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
        listpackEntry val;
        lpRandomPair(zsetobj->ptr, zsetsize, key, &val, 2);
        if (score) {
            if (val.sval) {
                *score = zzlStrtod(val.sval,val.slen);
            } else {
                *score = (double)val.lval;
            }
        }
    } else {
        serverPanic("Unknown zset encoding");
    }
}

/*-----------------------------------------------------------------------------
 * 有序集合命令
 *----------------------------------------------------------------------------*/

/* 此通用命令同时实现 ZADD 和 ZINCRBY。 */
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements, ch = 0;
    int scoreidx = 0;
    /* 以下变量用于跟踪命令在执行期间实际执行的操作，
     * 以回复客户端并触发 keyspace 变更的通知。 */
    int added = 0;      /* 新添加的元素数量。 */
    int updated = 0;    /* 更新了 score 的元素数量。 */
    int processed = 0;  /* 已处理的元素数量，对于 XX 等选项可能保持为零。 */

    /* 解析选项。最后 'scoreidx' 被设置为第一个 score-element 对的 score 的参数位置。 */
    scoreidx = 2;
    while(scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt,"nx")) flags |= ZADD_IN_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_IN_XX;
        else if (!strcasecmp(opt,"ch")) ch = 1; /* 返回已添加或已更新的元素数量。 */
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_IN_INCR;
        else if (!strcasecmp(opt,"gt")) flags |= ZADD_IN_GT;
        else if (!strcasecmp(opt,"lt")) flags |= ZADD_IN_LT;
        else break;
        scoreidx++;
    }

    /* 将选项转换为易于检查的变量。 */
    int incr = (flags & ZADD_IN_INCR) != 0;
    int nx = (flags & ZADD_IN_NX) != 0;
    int xx = (flags & ZADD_IN_XX) != 0;
    int gt = (flags & ZADD_IN_GT) != 0;
    int lt = (flags & ZADD_IN_LT) != 0;

    /* 选项之后，我们期望有偶数个参数，因为我们期望任意数量的 score-element 对。 */
    elements = c->argc-scoreidx;
    if (elements % 2 || !elements) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    elements /= 2; /* 现在它保存 score-element 对的数量。 */

    /* 检查不兼容的选项。 */
    if (nx && xx) {
        addReplyError(c,
            "XX and NX options at the same time are not compatible");
        return;
    }

    if ((gt && nx) || (lt && nx) || (gt && lt)) {
        addReplyError(c,
            "GT, LT, and/or NX options at the same time are not compatible");
        return;
    }
    /* 注意 XX 与 GT 或 LT 都兼容 */

    if (incr && elements > 1) {
        addReplyError(c,
            "INCR option supports a single increment-element pair");
        return;
    }

    /* 开始解析所有 score，我们需要在执行有序集合的添加之前发出任何语法错误，
     * 因为该命令应该要么完全执行，要么根本不执行。 */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[scoreidx+j*2],&scores[j],NULL)
            != C_OK) goto cleanup;
    }

    /* 查找 key，如果不存在则创建有序集合。 */
    zobj = lookupKeyWrite(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;
    if (zobj == NULL) {
        if (xx) goto reply_to_client; /* 没有 key + XX 选项：无事可做。 */
        zobj = zsetTypeCreate(elements, sdslen(c->argv[scoreidx+1]->ptr));
        dbAdd(c->db,key,zobj);
    } else {
        zsetTypeMaybeConvert(zobj, elements);
    }

    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = 0;

        ele = c->argv[scoreidx+1+j*2]->ptr;
        int retval = zsetAdd(zobj, score, ele, flags, &retflags, &newscore);
        if (retval == 0) {
            addReplyError(c,nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_OUT_ADDED) added++;
        if (retflags & ZADD_OUT_UPDATED) updated++;
        if (!(retflags & ZADD_OUT_NOP)) processed++;
        score = newscore;
    }
    server.dirty += (added+updated);

reply_to_client:
    if (incr) { /* ZINCRBY 或 INCR 选项。 */
        if (processed)
            addReplyDouble(c,score);
        else
            addReplyNull(c);
    } else { /* ZADD。 */
        addReplyLongLong(c,ch ? added+updated : added);
    }

cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

void zaddCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_NONE);
}

void zincrbyCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_INCR);
}

void zremCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    for (j = 2; j < c->argc; j++) {
        if (zsetDel(zobj,c->argv[j]->ptr)) deleted++;
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
            break;
        }
    }

    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_ZSET,"zrem",key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c,c->db,key);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zrange_type;

/* 实现 ZREMRANGEBYRANK、ZREMRANGEBYSCORE、ZREMRANGEBYLEX 命令。 */
void zremrangeGenericCommand(client *c, zrange_type rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;
    char *notify_type = NULL;

    /* 第 1 步：解析范围。 */
    if (rangetype == ZRANGE_RANK) {
        notify_type = "zremrangebyrank";
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        notify_type = "zremrangebyscore";
        if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        notify_type = "zremrangebylex";
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != C_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    } else {
        serverPanic("unknown rangetype %d", (int)rangetype);
    }

    /* 第 2 步：查找并（如需要）进行范围合理性检查。 */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* 清理索引。 */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* 不变式：start >= 0，所以当 end < 0 时此测试为真。
         * 当 start > end 或 start >= length 时范围为空。 */
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* 第 3 步：执行范围删除操作。 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        switch(rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictPauseAutoResize(zs->dict);
        switch(rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        dictResumeAutoResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        } else {
            dictShrinkIfNeeded(zs->dict);
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 第 4 步：通知和回复。 */
    if (deleted) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,notify_type,key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c,deleted);

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
            struct {
                unsigned char *lp;
                unsigned char *p;
            } lp;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* 在对 zsetopval 的下一次迭代中，使用 dirty flags 标记需要清理的指针。
 * long long 值的 dirty flag 是特殊的，因为 long long 值不需要清理。
 * 相反，它表示我们已经检查过 "ell" 保存 long long，
 * 或尝试将另一种表示转换为 long long 值。当成功时，
 * 也会设置 OPVAL_VALID_LL。 */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* 存储从迭代器检索的值。 */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* 私有缓冲区。 */
    sds ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else if (op->encoding == OBJ_ENCODING_LISTPACK) {
            it->lp.lp = op->subject->ptr;
            it->lp.p = lpFirst(it->lp.lp);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        /* 有序集合按相反顺序遍历，以优化
         * 在新列表中插入元素，如
         * ZDIFF/ZINTER/ZUNION */
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = lpSeek(it->zl.zl,-2);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = lpNext(it->zl.zl,it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->tail;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* 跳过 */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else if (op->encoding == OBJ_ENCODING_LISTPACK) {
            UNUSED(it);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            UNUSED(it); /* 跳过 */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* 跳过 */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

void zuiDiscardDirtyValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        sdsfree(val->ele);
        val->ele = NULL;
        val->flags &= ~OPVAL_DIRTY_SDS;
    }
}

unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        return setTypeSize(op->subject);
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 检查当前值是否有效。如果有效，则将其存储在传入的结构中，
 * 并移动到下一个元素。如果无效，则表示我们已经到达结构的末尾，
 * 可以中止。 */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    zuiDiscardDirtyValue(val);

    memset(val,0,sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            int64_t ell;

            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* 移动到下一个元素。 */
            it->is.ii++;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* 移动到下一个元素。 */
            it->ht.de = dictNext(it->ht.di);
        } else if (op->encoding == OBJ_ENCODING_LISTPACK) {
            if (it->lp.p == NULL)
                return 0;
            val->estr = lpGetValue(it->lp.p, &val->elen, &val->ell);
            val->score = 1.0;

            /* 移动到下一个元素。 */
            it->lp.p = lpNext(it->lp.lp, it->lp.p);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            /* 无需两者都检查，但最好显式说明。 */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            val->estr = lpGetValue(it->zl.eptr,&val->elen,&val->ell);
            val->score = zzlGetScore(it->zl.sptr);

            /* 移动到下一个元素（向后，参见 zuiInitIterator）。 */
            zzlPrev(it->zl.zl,&it->zl.eptr,&it->zl.sptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* 移动到下一个元素。（向后，参见 zuiInitIterator） */
            it->sl.node = it->sl.node->backward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele,sdslen(val->ele),&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* long long 已被设置，标记为有效。 */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char*)val->estr,val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_SDS;
    }
    return val->ele;
}

/* 这与 zuiSdsFromValue 不同，因为它返回一个新的 SDS 字符串，
 * 该字符串由调用者释放。 */
sds zuiNewSdsFromValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* 我们已经有一个可以返回的了！ */
        sds ele = val->ele;
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        return sdsdup(val->ele);
    } else if (val->estr) {
        return sdsnewlen((char*)val->estr,val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char*)val->ele;
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* 在 op 指向的 source 中查找 val 指向的值。找到时，
 * 返回 1 并将其 score 存储在 target 中。否则返回 0。 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        char *str = val->ele ? val->ele : (char *)val->estr;
        size_t len = val->ele ? sdslen(val->ele) : val->elen;
        if (setTypeIsMemberAux(op->subject, str, len, val->ell, val->ele != NULL)) {
            *score = 1.0;
            return 1;
        } else {
            return 0;
        }
    } else if (op->type == OBJ_ZSET) {
        zuiSdsFromValue(val);

        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score 已由 zzlFind 设置。 */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc*)s1);
    unsigned long second = zuiLength((zsetopsrc*)s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

static int zuiCompareByRevCardinality(const void *s1, const void *s2) {
    return zuiCompareByCardinality(s1, s2) * -1;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* 当一个变量是 +inf 而另一个是 -inf 时，
         * 两个 double 相加的结果为 NaN。当这些数字相加时，
         * 我们保持结果为 0.0 的约定。 */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* 安全网 */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

static size_t zsetDictGetMaxElementLength(dict *d, size_t *totallen) {
    dictIterator *di;
    dictEntry *de;
    size_t maxelelen = 0;

    di = dictGetIterator(d);

    while((de = dictNext(di)) != NULL) {
        sds ele = dictGetKey(de);
        if (sdslen(ele) > maxelelen) maxelelen = sdslen(ele);
        if (totallen)
            (*totallen) += sdslen(ele);
    }

    dictReleaseIterator(di);

    return maxelelen;
}

static void zdiffAlgorithm1(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* DIFF 算法 1：
     *
     * 我们通过迭代第一个集合的所有元素来执行 diff，
     * 并且仅当该元素不存在于所有其他集合中时才将其添加到目标集合。
     *
     * 这样我们最多执行 N*M 次操作，其中 N 是第一个集合的大小，
     * M 是集合的数量。
     *
     * 将结果元素添加到目标集合还存在 O(K*log(K)) 的开销，
     * 其中 K 是目标集合的最终大小。
     *
     * 该算法的最终复杂度为 O(N*M + K*log(K))。 */
    int j;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    /* 使用算法 1 时，最好按大小递减的顺序对待减的集合进行排序，
     * 这样我们更有可能尽快找到重复的元素。 */
    qsort(src+1,setnum-1,sizeof(zsetopsrc),zuiCompareByRevCardinality);

    memset(&zval, 0, sizeof(zval));
    zuiInitIterator(&src[0]);
    while (zuiNext(&src[0],&zval)) {
        double value;
        int exists = 0;

        for (j = 1; j < setnum; j++) {
            /* 访问我们正在迭代的 zset 不安全，因此显式检查相等的对象。
             * 由于我们已经在 zsetChooseDiffAlgorithm 函数中检查了重复集合，
             * 所以此检查实际上不再需要，但我们将其保留以防未来需要。 */
            if (src[j].subject == src[0].subject ||
                zuiFind(&src[j],&zval,&value)) {
                exists = 1;
                break;
            }
        }

        if (!exists) {
            tmp = zuiNewSdsFromValue(&zval);
            znode = zslInsert(dstzset->zsl,zval.score,tmp);
            dictAdd(dstzset->dict,tmp,&znode->score);
            if (sdslen(tmp) > *maxelelen) *maxelelen = sdslen(tmp);
            (*totelelen) += sdslen(tmp);
        }
    }
    zuiClearIterator(&src[0]);
}


static void zdiffAlgorithm2(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* DIFF 算法 2：
     *
     * 将第一个集合的所有元素添加到辅助集合中。
     * 然后从中移除所有后续集合的所有元素。
     *

     * 该算法的复杂度为 O(L + (N-K)log(N))，其中 L 是每个集合中所有元素的总和，
     * N 是第一个集合的大小，K 是结果集合的大小。
     *
     * 注意，从 (L-N) 次字典搜索中，(N-K) 次会进入 zsetRemoveFromSkiplist，
     * 其代价为 log(N)
     *
     * 最后还存在 O(K) 的代价用于查找最大元素大小，
     * 但这不会改变算法的复杂度，因为 K < L，且 O(2L) 与 O(L) 相同。 */
    int j;
    int cardinality = 0;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    for (j = 0; j < setnum; j++) {
        if (zuiLength(&src[j]) == 0) continue;

        memset(&zval, 0, sizeof(zval));
        zuiInitIterator(&src[j]);
        while (zuiNext(&src[j],&zval)) {
            if (j == 0) {
                tmp = zuiNewSdsFromValue(&zval);
                znode = zslInsert(dstzset->zsl,zval.score,tmp);
                dictAdd(dstzset->dict,tmp,&znode->score);
                cardinality++;
            } else {
                dictPauseAutoResize(dstzset->dict);
                tmp = zuiSdsFromValue(&zval);
                if (zsetRemoveFromSkiplist(dstzset, tmp)) {
                    cardinality--;
                }
                dictResumeAutoResize(dstzset->dict);
            }

            /* 如果结果集合为空则退出，因为进一步删除
                * 元素将没有任何效果。 */
            if (cardinality == 0) break;
        }
        zuiClearIterator(&src[j]);

        if (cardinality == 0) break;
    }

    /* 在删除多个元素后，如果需要则调整 dict 大小 */
    dictShrinkIfNeeded(dstzset->dict);

    /* 使用此算法，我们无法在迭代过程中计算最大元素，
     * 必须之后遍历所有元素才能找到最大元素。 */
    *maxelelen = zsetDictGetMaxElementLength(dstzset->dict, totelelen);
}

static int zsetChooseDiffAlgorithm(zsetopsrc *src, long setnum) {
    int j;

    /* 选择要使用的 DIFF 算法。
     *
     * 算法 1 是 O(N*M + K*log(K))，其中 N 是第一个集合的大小，
     * M 是集合的总数，K 是结果集合的大小。
     *
     * 算法 2 是 O(L + (N-K)log(N))，其中 L 是所有集合中的元素总数，
     * N 是第一个集合的大小，K 是结果集合的大小。
     *
     * 我们在此处根据当前输入计算最佳选择。 */
    long long algo_one_work = 0;
    long long algo_two_work = 0;

    for (j = 0; j < setnum; j++) {
        /* 如果任何其他集合等于第一个集合，则无事可做，
         * 因为无论如何都会删除所有元素。 */
        if (j > 0 && src[0].subject == src[j].subject) {
            return 0;
        }

        algo_one_work += zuiLength(&src[0]);
        algo_two_work += zuiLength(&src[j]);
    }

    /* 如果存在公共元素，算法 1 具有更好的常数时间和更少的操作。
     * 给它一些优势。 */
    algo_one_work /= 2;
    return (algo_one_work <= algo_two_work) ? 1 : 2;
}

static void zdiff(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* 如果最小的输入为空，则跳过所有操作。 */
    if (zuiLength(&src[0]) > 0) {
        int diff_algo = zsetChooseDiffAlgorithm(src, setnum);
        if (diff_algo == 1) {
            zdiffAlgorithm1(src, setnum, dstzset, maxelelen, totelelen);
        } else if (diff_algo == 2) {
            zdiffAlgorithm2(src, setnum, dstzset, maxelelen, totelelen);
        } else if (diff_algo != 0) {
            serverPanic("Unknown algorithm");
        }
    }
}

dictType setAccumulatorDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* 调用 zunionInterDiffGenericCommand() 函数以实现以下命令：
 * ZUNION、ZINTER、ZDIFF、ZUNIONSTORE、ZINTERSTORE、ZDIFFSTORE、ZINTERCARD。
 *
 * 'numkeysIndex' 参数为 key 数量的位置。
 * 对于 ZUNION/ZINTER/ZDIFF 命令，该值为 1；
 * 对于 ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE 命令，该值为 2。
 *
 * 'op' 为 SET_OP_INTER、SET_OP_UNION 或 SET_OP_DIFF。
 *
 * 'cardinality_only' 目前仅在 'op' 为 SET_OP_INTER 时适用。
 * 用于 SINTERCARD，仅以最小处理和内存开销返回基数。
 */
void zunionInterDiffGenericCommand(client *c, robj *dstkey, int numkeysIndex, int op,
                                   int cardinality_only) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0, totelelen = 0;
    robj *dstobj = NULL;
    zset *dstzset = NULL;
    zskiplistNode *znode;
    int withscores = 0;
    unsigned long cardinality = 0;
    long limit = 0; /* 达到限制后停止搜索。0 表示无限制。 */

    /* 期望提供 setnum 个输入 key */
    if ((getLongFromObjectOrReply(c, c->argv[numkeysIndex], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyErrorFormat(c,
            "at least 1 input key is needed for '%s' command", c->cmd->fullname);
        return;
    }

    /* 测试预期的 key 数量是否会溢出 */
    if (setnum > (c->argc-(numkeysIndex+1))) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 尝试分配 src 表，并在内存不足时中止。 */
    src = ztrycalloc(sizeof(zsetopsrc) * setnum);
    if (src == NULL) {
        addReplyError(c, "Insufficient memory, failed allocating transient memory, too many args.");
        return;
    }

    /* 读取用作输入的 key */
    for (i = 0, j = numkeysIndex+1; i < setnum; i++, j++) {
        robj *obj = lookupKeyRead(c->db, c->argv[j]);
        if (obj != NULL) {
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReplyErrorObject(c,shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* 默认所有权重为 1。 */
        src[i].weight = 1.0;
    }

    /* 解析可选的额外参数 */
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (op != SET_OP_DIFF && !cardinality_only &&
                remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr,"weights"))
            {
                j++; remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != C_OK)
                    {
                        zfree(src);
                        return;
                    }
                }
            } else if (op != SET_OP_DIFF && !cardinality_only &&
                       remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr,"aggregate"))
            {
                j++; remaining--;
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;
            } else if (remaining >= 1 &&
                       !dstkey && !cardinality_only &&
                       !strcasecmp(c->argv[j]->ptr,"withscores"))
            {
                j++; remaining--;
                withscores = 1;
            } else if (cardinality_only && remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr, "limit"))
            {
                j++; remaining--;
                if (getPositiveLongFromObjectOrReply(c, c->argv[j], &limit,
                                                     "LIMIT can't be negative") != C_OK)
                {
                    zfree(src);
                    return;
                }
                j++; remaining--;
            } else {
                zfree(src);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    if (op != SET_OP_DIFF) {
        /* 按大小从小到大排序集合，这将提高我们算法的性能 */
        qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);
    }

    /* 我们需要一个临时 zset 对象来存储我们的 union/inter/diff。
     * 如果 dstkey 不为 NULL（即我们在 ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE 操作中），
     * 那么该 zset 对象将成为要设置到目标 key 的结果对象。
     * 在 SINTERCARD 的情况下，我们不需要临时对象，因此可以避免创建它。 */
    if (!cardinality_only) {
        dstobj = createZsetObject();
        dstzset = dstobj->ptr;
    }
    memset(&zval, 0, sizeof(zval));

    if (op == SET_OP_INTER) {
        /* 如果最小的输入为空，则跳过所有操作。 */
        if (zuiLength(&src[0]) > 0) {
            /* 前提条件：由于 src[0] 非空且输入按大小排序，
             * 所有 src[i > 0] 也都非空。 */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                for (j = 1; j < setnum; j++) {
                    /* 访问我们正在迭代的 zset 不安全，因此显式检查相等的对象。 */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else {
                        break;
                    }
                }

                /* 仅当存在于每个输入中时才继续。 */
                if (j == setnum && cardinality_only) {
                    cardinality++;

                    /* 达到限制后我们停止搜索。 */
                    if (limit && cardinality >= (unsigned long)limit) {
                        /* 在我们中断 zuiNext 循环之前进行清理。 */
                        zuiDiscardDirtyValue(&zval);
                        break;
                    }
                } else if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    totelelen += sdslen(tmp);
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }
    } else if (op == SET_OP_UNION) {
        dict *accumulator = dictCreate(&setAccumulatorDictType);
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* 我们的 union 至少与最大的集合一样大。
             * 尽快调整字典大小以避免无用的 rehash。 */
            dictExpand(accumulator,zuiLength(&src[setnum-1]));
        }

        /* 第 1 步：通过逐个迭代有序集合，创建元素 -> 聚合分数的字典。 */
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i],&zval)) {
                /* 初始化值 */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* 在累积字典中搜索该元素。 */
                de = dictAddRaw(accumulator,zuiSdsFromValue(&zval),&existing);
                /* 如果没有，我们需要创建一个新条目。 */
                if (!existing) {
                    tmp = zuiNewSdsFromValue(&zval);
                    /* 记住遇到的单个最长元素，
                     * 以了解最后是否可能转换为 listpack。 */
                     totelelen += sdslen(tmp);
                     if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* 用其初始 score 更新元素。 */
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de,score);
                } else {
                    /* 使用当前有序集合中找到的元素新实例的 score 更新 score。
                     *
                     * 这里我们直接访问 union 中 dictEntry 的 double 值，
                     * 因为与使用 getDouble/setDouble API 相比，这是一个很大的加速。 */
                    double *existing_score_ptr = dictGetDoubleValPtr(existing);
                    zunionInterAggregate(existing_score_ptr, score, aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* 第 2 步：将字典转换为最终的有序集合。 */
        di = dictGetIterator(accumulator);

        /* 现在我们已经知道结果有序集合的最终大小，
         * 让我们将有序集合内嵌的字典调整为正确的大小，
         * 以节省 rehash 时间。 */
        dictExpand(dstzset->dict,dictSize(accumulator));

        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl,score,ele);
            dictAdd(dstzset->dict,ele,&znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    } else if (op == SET_OP_DIFF) {
        zdiff(src, setnum, dstzset, &maxelelen, &totelelen);
    } else {
        serverPanic("Unknown operator");
    }

    if (dstkey) {
        if (dstzset->zsl->length) {
            zsetConvertToListpackIfNeeded(dstobj, maxelelen, totelelen);
            setKey(c, c->db, dstkey, dstobj, 0);
            addReplyLongLong(c, zsetLength(dstobj));
            notifyKeyspaceEvent(NOTIFY_ZSET,
                                (op == SET_OP_UNION) ? "zunionstore" :
                                    (op == SET_OP_INTER ? "zinterstore" : "zdiffstore"),
                                dstkey, c->db->id);
            server.dirty++;
        } else {
            addReply(c, shared.czero);
            if (dbDelete(c->db, dstkey)) {
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
                server.dirty++;
            }
        }
        decrRefCount(dstobj);
    } else if (cardinality_only) {
        addReplyLongLong(c, cardinality);
    } else {
        unsigned long length = dstzset->zsl->length;
        zskiplist *zsl = dstzset->zsl;
        zskiplistNode *zn = zsl->header->level[0].forward;
        /* 在 WITHSCORES 的情况下，在 RESP2 中使用单个数组进行响应，
         * 在 RESP3 中使用嵌套数组。我们不能使用 map 响应类型，
         * 因为客户端库需要知道要遵循顺序。 */
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, length*2);
        else
            addReplyArrayLen(c, length);

        while (zn != NULL) {
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            addReplyBulkCBuffer(c,zn->ele,sdslen(zn->ele));
            if (withscores) addReplyDouble(c,zn->score);
            zn = zn->level[0].forward;
        }
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstobj, -1) :
                                          decrRefCount(dstobj);
    }
    zfree(src);
}

/* ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] */
void zunionstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_UNION, 0);
}

/* ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] */
void zinterstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_INTER, 0);
}

/* ZDIFFSTORE destination numkeys key [key ...] */
void zdiffstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_DIFF, 0);
}

/* ZUNION numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] */
void zunionCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_UNION, 0);
}

/* ZINTER numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] */
void zinterCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER, 0);
}

/* ZINTERCARD numkeys key [key ...] [LIMIT limit] */
void zinterCardCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER, 1);
}

/* ZDIFF numkeys key [key ...] [WITHSCORES] */
void zdiffCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_DIFF, 0);
}

typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zrange_direction;

typedef enum {
    ZRANGE_CONSUMER_TYPE_CLIENT = 0,
    ZRANGE_CONSUMER_TYPE_INTERNAL
} zrange_consumer_type;

typedef struct zrange_result_handler zrange_result_handler;

typedef void (*zrangeResultBeginFunction)(zrange_result_handler *c, long length);
typedef void (*zrangeResultFinalizeFunction)(
    zrange_result_handler *c, size_t result_count);
typedef void (*zrangeResultEmitCBufferFunction)(
    zrange_result_handler *c, const void *p, size_t len, double score);
typedef void (*zrangeResultEmitLongLongFunction)(
    zrange_result_handler *c, long long ll, double score);

void zrangeGenericCommand (zrange_result_handler *handler, int argc_start, int store,
                           zrange_type rangetype, zrange_direction direction);

/* ZRANGE/ZRANGESTORE 通用实现的接口结构。
 * 该接口有一个实现向客户端发送 RESP 响应，
 * 还有另一个实现将范围结果存储到 zset 对象中。 */
struct zrange_result_handler {
    zrange_consumer_type                 type;
    client                              *client;
    robj                                *dstkey;
    robj                                *dstobj;
    void                                *userdata;
    int                                  withscores;
    int                                  should_emit_array_length;
    zrangeResultBeginFunction            beginResultEmission;
    zrangeResultFinalizeFunction         finalizeResultEmission;
    zrangeResultEmitCBufferFunction      emitResultFromCBuffer;
    zrangeResultEmitLongLongFunction     emitResultFromLongLong;
};

/* 用于响应 ZRANGE 给客户端的结果处理方法。
 * length 可用于预先提供结果长度（避免延迟回复开销）。
 * 如果结果长度事先未知，length 可以设置为 -1。
 */
static void zrangeResultBeginClient(zrange_result_handler *handler, long length) {
    if (length > 0) {
        /* 在 WITHSCORES 的情况下，在 RESP2 中使用单个数组进行响应，
        * 在 RESP3 中使用嵌套数组。我们不能使用 map 响应类型，
        * 因为客户端库需要知道要遵循顺序。 */
        if (handler->withscores && (handler->client->resp == 2)) {
            length *= 2;
        }
        addReplyArrayLen(handler->client, length);
        handler->userdata = NULL;
        return;
    }
    handler->userdata = addReplyDeferredLen(handler->client);
}

static void zrangeResultEmitCBufferToClient(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkCBuffer(handler->client, value, value_length_in_bytes);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

static void zrangeResultEmitLongLongToClient(zrange_result_handler *handler,
    long long value, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkLongLong(handler->client, value);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

static void zrangeResultFinalizeClient(zrange_result_handler *handler,
    size_t result_count)
{
    /* 如果一开始就知道回复大小，则无需执行其他操作 */
    if (!handler->userdata)
        return;
    /* 在 WITHSCORES 的情况下，在 RESP2 中使用单个数组进行响应，
     * 在 RESP3 中使用嵌套数组。我们不能使用 map 响应类型，
     * 因为客户端库需要知道要遵循顺序。 */
    if (handler->withscores && (handler->client->resp == 2)) {
        result_count *= 2;
    }

    setDeferredArrayLen(handler->client, handler->userdata, result_count);
}

/* 用于将 ZRANGESTORE 存储到 zset 的结果处理方法。 */
static void zrangeResultBeginStore(zrange_result_handler *handler, long length)
{
    handler->dstobj = zsetTypeCreate(length >= 0 ? length : 0, 0);
}

static void zrangeResultEmitCBufferForStore(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsnewlen(value, value_length_in_bytes);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

static void zrangeResultEmitLongLongForStore(zrange_result_handler *handler,
    long long value, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsfromlonglong(value);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

static void zrangeResultFinalizeStore(zrange_result_handler *handler, size_t result_count)
{
    if (result_count) {
        setKey(handler->client, handler->client->db, handler->dstkey, handler->dstobj, 0);
        addReplyLongLong(handler->client, result_count);
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrangestore", handler->dstkey, handler->client->db->id);
        server.dirty++;
    } else {
        addReply(handler->client, shared.czero);
        if (dbDelete(handler->client->db, handler->dstkey)) {
            signalModifiedKey(handler->client, handler->client->db, handler->dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", handler->dstkey, handler->client->db->id);
            server.dirty++;
        }
    }
    decrRefCount(handler->dstobj);
}

/* 使用请求的类型初始化消费者接口类型。 */
static void zrangeResultHandlerInit(zrange_result_handler *handler,
    client *client, zrange_consumer_type type)
{
    memset(handler, 0, sizeof(*handler));

    handler->client = client;

    switch (type) {
    case ZRANGE_CONSUMER_TYPE_CLIENT:
        handler->beginResultEmission = zrangeResultBeginClient;
        handler->finalizeResultEmission = zrangeResultFinalizeClient;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferToClient;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongToClient;
        break;

    case ZRANGE_CONSUMER_TYPE_INTERNAL:
        handler->beginResultEmission = zrangeResultBeginStore;
        handler->finalizeResultEmission = zrangeResultFinalizeStore;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferForStore;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongForStore;
        break;
    }
}

static void zrangeResultHandlerScoreEmissionEnable(zrange_result_handler *handler) {
    handler->withscores = 1;
    handler->should_emit_array_length = (handler->client->resp > 2);
}

static void zrangeResultHandlerDestinationKeySet (zrange_result_handler *handler,
    robj *dstkey)
{
    handler->dstkey = dstkey;
}

/* 此命令实现 ZRANGE、ZREVRANGE。 */
void genericZrangebyrankCommand(zrange_result_handler *handler,
    robj *zobj, long start, long end, int withscores, int reverse) {

    client *c = handler->client;
    long llen;
    long rangelen;
    size_t result_cardinality;

    /* 清理索引。 */
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;


    /* 不变式：start >= 0，所以当 end < 0 时此测试为真。
     * 当 start > end 或 start >= length 时范围为空。 */
    if (start > end || start >= llen) {
        handler->beginResultEmission(handler, 0);
        handler->finalizeResultEmission(handler, 0);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;
    result_cardinality = rangelen;

    handler->beginResultEmission(handler, rangelen);
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score = 0.0;

        if (reverse)
            eptr = lpSeek(zl,-2-(2*start));
        else
            eptr = lpSeek(zl,2*start);

        serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = lpNext(zl,eptr);

        while (rangelen--) {
            serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            vstr = lpGetValue(eptr,&vlen,&vlong);

            if (withscores) /* 如果 score 会被忽略，则不必费心提取它。 */
                score = zzlGetScore(sptr);

            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* 在执行 log(N) 查找之前，检查起始点是否平凡。 */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--) {
            serverAssertWithInfo(c,zobj,ln != NULL);
            sds ele = ln->ele;
            handler->emitResultFromCBuffer(handler, ele, sdslen(ele), ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, result_cardinality);
}

/* ZRANGESTORE <dst> <src> <min> <max> [BYSCORE | BYLEX] [REV] [LIMIT offset count] */
void zrangestoreCommand (client *c) {
    robj *dstkey = c->argv[1];
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_INTERNAL);
    zrangeResultHandlerDestinationKeySet(&handler, dstkey);
    zrangeGenericCommand(&handler, 2, 1, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count] */
void zrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZREVRANGE <key> <start> <stop> [WITHSCORES] */
void zrevrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_RANK, ZRANGE_DIRECTION_REVERSE);
}

/* 此命令实现 ZRANGEBYSCORE、ZREVRANGEBYSCORE。 */
void genericZrangebyscoreCommand(zrange_result_handler *handler,
    zrangespec *range, robj *zobj, long offset, long limit,
    int reverse) {
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler, -1);

    /* 对于无效的 offset，直接返回。 */
    if (offset < 0 || (offset > 0 && offset >= (long)zsetLength(zobj))) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* 如果是反向的，则将范围内最后一个节点作为起点。 */
        if (reverse) {
            eptr = zzlLastInRange(zl,range);
        } else {
            eptr = zzlFirstInRange(zl,range);
        }

        /* 获取第一个元素的 score 指针。 */
        if (eptr)
            sptr = lpNext(zl,eptr);

        /* 如果有 offset，只需遍历该数量的元素而不检查 score，
         * 因为这将在下一个循环中完成。 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            double score = zzlGetScore(sptr);

            /* 当节点不再在范围内时中止。 */
            if (reverse) {
                if (!zslValueGteMin(score,range)) break;
            } else {
                if (!zslValueLteMax(score,range)) break;
            }

            vstr = lpGetValue(eptr,&vlen,&vlong);
            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* 如果是反向的，则将范围内最后一个节点作为起点。 */
        if (reverse) {
            ln = zslNthInRange(zsl,range,-offset-1);
        } else {
            ln = zslNthInRange(zsl,range,offset);
        }

        while (ln && limit--) {
            /* 当节点不再在范围内时中止。 */
            if (reverse) {
                if (!zslValueGteMin(ln->score,range)) break;
            } else {
                if (!zslValueLteMax(ln->score,range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
void zrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYSCORE <key> <max> <min> [WITHSCORES] [LIMIT offset count] */
void zrevrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_REVERSE);
}

void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* 解析范围参数 */
    if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* 查找有序集合 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) return;

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* 使用范围内第一个元素作为起点 */
        eptr = zzlFirstInRange(zl,&range);

        /* 没有 "第一个" 元素 */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* 第一个元素在范围内 */
        sptr = lpNext(zl,eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* 遍历范围内的元素 */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* 当节点不再在范围内时中止。 */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* 查找范围内第一个元素 */
        zn = zslNthInRange(zsl, &range, 0);

        /* 使用第一个元素的 rank（如果有）确定初步计数 */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* 查找范围内最后一个元素 */
            zn = zslNthInRange(zsl, &range, -1);

            /* 使用最后一个元素的 rank（如果有）确定实际计数 */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* 解析范围参数 */
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* 查找有序集合 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* 使用范围内第一个元素作为起点 */
        eptr = zzlFirstInLexRange(zl,&range);

        /* 没有 "第一个" 元素 */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* 第一个元素在范围内 */
        sptr = lpNext(zl,eptr);
        serverAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* 遍历范围内的元素 */
        while (eptr) {
            /* 当节点不再在范围内时中止。 */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* 查找范围内第一个元素 */
        zn = zslNthInLexRange(zsl, &range, 0);

        /* 使用第一个元素的 rank（如果有）确定初步计数 */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* 查找范围内最后一个元素 */
            zn = zslNthInLexRange(zsl, &range, -1);

            /* 使用最后一个元素的 rank（如果有）确定实际计数 */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* 此命令实现 ZRANGEBYLEX、ZREVRANGEBYLEX。 */
void genericZrangebylexCommand(zrange_result_handler *handler,
    zlexrangespec *range, robj *zobj, int withscores, long offset, long limit,
    int reverse)
{
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler, -1);

    /* 对于无效的 offset，直接返回。 */
    if (offset < 0 || (offset > 0 && offset >= (long)zsetLength(zobj))) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* 如果是反向的，则将范围内最后一个节点作为起点。 */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,range);
        } else {
            eptr = zzlFirstInLexRange(zl,range);
        }

        /* 获取第一个元素的 score 指针。 */
        if (eptr)
            sptr = lpNext(zl,eptr);

        /* 如果有 offset，只需遍历该数量的元素而不检查 score，
         * 因为这将在下一个循环中完成。 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            double score = 0;
            if (withscores) /* 如果 score 会被忽略，则不必费心提取它。 */
                score = zzlGetScore(sptr);

            /* 当节点不再在范围内时中止。 */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,range)) break;
            }

            vstr = lpGetValue(eptr,&vlen,&vlong);
            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* 如果是反向的，则将范围内最后一个节点作为起点。 */
        if (reverse) {
            ln = zslNthInLexRange(zsl,range,-offset-1);
        } else {
            ln = zslNthInLexRange(zsl,range,offset);
        }

        while (ln && limit--) {
            /* 当节点不再在范围内时中止。 */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele,range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele,range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
void zrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYLEX <key> <max> <min> [LIMIT offset count] */
void zrevrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_REVERSE);
}

/**
 * 此函数处理 ZRANGE 和 ZRANGESTORE，以及已弃用的
 * Z[REV]RANGE[BYSCORE|BYLEX] 命令。
 *
 * 简单的 ZRANGE 和 ZRANGESTORE 在 rangetype 和 direction 中可以使用 _AUTO，
 * 其他命令则传递显式值。
 *
 * argc_start 指向 src key 参数，因此以下语法类似于：
 * <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
 */
void zrangeGenericCommand(zrange_result_handler *handler, int argc_start, int store,
                          zrange_type rangetype, zrange_direction direction)
{
    client *c = handler->client;
    robj *key = c->argv[argc_start];
    robj *zobj;
    zrangespec range;
    zlexrangespec lexrange;
    int minidx = argc_start + 1;
    int maxidx = argc_start + 2;

    /* 所有命令共有的选项 */
    long opt_start = 0;
    long opt_end = 0;
    int opt_withscores = 0;
    long opt_offset = 0;
    long opt_limit = -1;

    /* 第 1 步：跳过 <src> <min> <max> 参数并解析其余的可选参数。 */
    for (int j=argc_start + 3; j < c->argc; j++) {
        int leftargs = c->argc-j-1;
        if (!store && !strcasecmp(c->argv[j]->ptr,"withscores")) {
            opt_withscores = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            if ((getLongFromObjectOrReply(c, c->argv[j+1], &opt_offset, NULL) != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j+2], &opt_limit, NULL) != C_OK))
            {
                return;
            }
            j += 2;
        } else if (direction == ZRANGE_DIRECTION_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"rev"))
        {
            direction = ZRANGE_DIRECTION_REVERSE;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"bylex"))
        {
            rangetype = ZRANGE_LEX;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"byscore"))
        {
            rangetype = ZRANGE_SCORE;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* 如果未被参数覆盖，则使用默认值。 */
    if (direction == ZRANGE_DIRECTION_AUTO)
        direction = ZRANGE_DIRECTION_FORWARD;
    if (rangetype == ZRANGE_AUTO)
        rangetype = ZRANGE_RANK;

    /* 检查冲突的参数。 */
    if (opt_limit != -1 && rangetype == ZRANGE_RANK) {
        addReplyError(c,"syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return;
    }
    if (opt_withscores && rangetype == ZRANGE_LEX) {
        addReplyError(c,"syntax error, WITHSCORES not supported in combination with BYLEX");
        return;
    }

    if (direction == ZRANGE_DIRECTION_REVERSE &&
        ((ZRANGE_SCORE == rangetype) || (ZRANGE_LEX == rangetype)))
    {
        /* 范围以 [max,min] 形式给出 */
        int tmp = maxidx;
        maxidx = minidx;
        minidx = tmp;
    }

    /* 第 2 步：解析范围。 */
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        /* Z[REV]RANGE, ZRANGESTORE [REV]RANGE */
        if ((getLongFromObjectOrReply(c, c->argv[minidx], &opt_start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c, c->argv[maxidx], &opt_end,NULL) != C_OK))
        {
            return;
        }
        break;

    case ZRANGE_SCORE:
        /* Z[REV]RANGEBYSCORE, ZRANGESTORE [REV]RANGEBYSCORE */
        if (zslParseRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
            addReplyError(c, "min or max is not a float");
            return;
        }
        break;

    case ZRANGE_LEX:
        /* Z[REV]RANGEBYLEX, ZRANGESTORE [REV]RANGEBYLEX */
        if (zslParseLexRange(c->argv[minidx], c->argv[maxidx], &lexrange) != C_OK) {
            addReplyError(c, "min or max not valid string range item");
            return;
        }
        break;
    }

    if (opt_withscores || store) {
        zrangeResultHandlerScoreEmissionEnable(handler);
    }

    /* 第 3 步：查找 key 并获取范围。 */
    zobj = lookupKeyRead(c->db, key);
    if (zobj == NULL) {
        if (store) {
            handler->beginResultEmission(handler, -1);
            handler->finalizeResultEmission(handler, 0);
        } else {
            addReply(c, shared.emptyarray);
        }
        goto cleanup;
    }

    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    /* 第 4 步：传递给特定于命令的处理程序。 */
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        genericZrangebyrankCommand(handler, zobj, opt_start, opt_end,
            opt_withscores || store, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_SCORE:
        genericZrangebyscoreCommand(handler, &range, zobj, opt_offset,
            opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_LEX:
        genericZrangebylexCommand(handler, &lexrange, zobj, opt_withscores || store,
            opt_offset, opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;
    }

    /* 我们不在此处直接返回，而是直接进入 clean-up。 */

cleanup:

    if (rangetype == ZRANGE_LEX) {
        zslFreeLexRange(&lexrange);
    }
}

void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    addReplyLongLong(c,zsetLength(zobj));
}

void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    if (zsetScore(zobj,c->argv[2]->ptr,&score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c,score);
    }
}

void zmscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;
    zobj = lookupKeyRead(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) return;

    addReplyArrayLen(c,c->argc - 2);
    for (int j = 2; j < c->argc; j++) {
        /* Treat a missing set the same way as an empty set */
        if (zobj == NULL || zsetScore(zobj,c->argv[j]->ptr,&score) == C_ERR) {
            addReplyNull(c);
        } else {
            addReplyDouble(c,score);
        }
    }
}

void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    robj* reply;
    long rank;
    int opt_withscore = 0;
    double score;

    if (c->argc > 4) {
        addReplyErrorArity(c);
        return;
    }
    if (c->argc > 3) {
        if (!strcasecmp(c->argv[3]->ptr, "withscore")) {
            opt_withscore = 1;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }
    reply = opt_withscore ? shared.nullarray[c->resp] : shared.null[c->resp];
    if ((zobj = lookupKeyReadOrReply(c, key, reply)) == NULL || checkType(c, zobj, OBJ_ZSET)) {
        return;
    }
    serverAssertWithInfo(c, ele, sdsEncodedObject(ele));
    rank = zsetRank(zobj, ele->ptr, reverse, opt_withscore ? &score : NULL);
    if (rank >= 0) {
        if (opt_withscore) {
            addReplyArrayLen(c, 2);
        }
        addReplyLongLong(c, rank);
        if (opt_withscore) {
            addReplyDouble(c, score);
        }
    } else {
        if (opt_withscore) {
            addReplyNullArray(c);
        } else {
            addReplyNull(c);
        }
    }
}

void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(client *c) {
    robj *o;
    unsigned long long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX and ZMPOP. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN, BZPOPMAX and BZMPOP.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 * Or in ZMPOP/BZMPOP, because we also can take multiple keys.
 *
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * 'use_nested_array' 为 false 时，它生成一个扁平数组（带或不带 key 名称）。
 * 为 true 时，它生成一个嵌套的 2 级数组，其中包含 field + score 对，
 * 设置 emitkey 时为 3 级。
 *
 * 'reply_nil_when_empty' 为 true 时，如果我们无法弹出任何元素，则回复 NIL。
 * 像在 ZMPOP/BZMPOP 中，我们使用包含 key 名称和 member + score 对的结构化
 * 嵌套数组进行回复。在这些命令中，当我们没有结果时，我们回复 null。
 * 否则在 ZPOPMIN/ZPOPMAX 中，默认回复空数组。
 *
 * 'deleted' 是一个可选的输出参数，用于指示此函数是否删除了 key。
 * */
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey,
                        long count, int use_nested_array, int reply_nil_when_empty, int *deleted) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;

    if (deleted) *deleted = 0;

    /* Check type and break on the first error, otherwise identify candidate. */
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db,key);
        if (!zobj) continue;
        if (checkType(c,zobj,OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty. */
    if (!zobj) {
        if (reply_nil_when_empty) {
            addReplyNullArray(c);
        } else {
            addReply(c,shared.emptyarray);
        }
        return;
    }

    if (count == 0) {
        /* ZPOPMIN/ZPOPMAX with count 0. */
        addReply(c, shared.emptyarray);
        return;
    }

    long result_count = 0;

    /* When count is -1, we need to correct it to 1 for plain single pop. */
    if (count == -1) count = 1;

    long llen = zsetLength(zobj);
    long rangelen = (count > llen) ? llen : count;

    if (!use_nested_array && !emitkey) {
        /* ZPOPMIN/ZPOPMAX with or without COUNT option in RESP2. */
        addReplyArrayLen(c, rangelen * 2);
    } else if (use_nested_array && !emitkey) {
        /* ZPOPMIN/ZPOPMAX with COUNT option in RESP3. */
        addReplyArrayLen(c, rangelen);
    } else if (!use_nested_array && emitkey) {
        /* BZPOPMIN/BZPOPMAX in RESP2 and RESP3. */
        addReplyArrayLen(c, rangelen * 2 + 1);
        addReplyBulk(c, key);
    } else if (use_nested_array && emitkey) {
        /* ZMPOP/BZMPOP in RESP2 and RESP3. */
        addReplyArrayLen(c, 2);
        addReplyBulk(c, key);
        addReplyArrayLen(c, rangelen);
    }

    /* Remove the element. */
    do {
        if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            eptr = lpSeek(zl,where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c,zobj,eptr != NULL);
            vstr = lpGetValue(eptr,&vlen,&vlong);
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr,vlen);

            /* Get the score. */
            sptr = lpNext(zl,eptr);
            serverAssertWithInfo(c,zobj,sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            zln = (where == ZSET_MAX ? zsl->tail :
                                       zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            serverAssertWithInfo(c,zobj,zln != NULL);
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        serverAssertWithInfo(c,zobj,zsetDel(zobj,ele));
        server.dirty++;

        if (result_count == 0) { /* Do this only for the first iteration. */
            char *events[2] = {"zpopmin","zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET,events[where],key,c->db->id);
        }

        if (use_nested_array) {
            addReplyArrayLen(c,2);
        }
        addReplyBulkCBuffer(c,ele,sdslen(ele));
        addReplyDouble(c,score);
        sdsfree(ele);
        ++result_count;
    } while(--rangelen);

    /* Remove the key, if indeed needed. */
    if (zsetLength(zobj) == 0) {
        if (deleted) *deleted = 1;

        dbDelete(c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }
    signalModifiedKey(c,c->db,key);

    if (c->cmd->proc == zmpopCommand) {
        /* Always replicate it as ZPOP[MIN|MAX] with COUNT option instead of ZMPOP. */
        robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
        rewriteClientCommandVector(c, 3,
                                   (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                   key, count_obj);
        decrRefCount(count_obj);
    }
}

/* ZPOPMIN/ZPOPMAX key [<count>] */
void zpopMinMaxCommand(client *c, int where) {
    if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    long count = -1; /* -1 for plain single pop. */
    if (c->argc == 3 && getPositiveLongFromObjectOrReply(c, c->argv[2], &count, NULL) != C_OK)
        return;

    /* Respond with a single (flat) array in RESP2 or if count is -1
     * (returning a single element). In RESP3, when count > 0 use nested array. */
    int use_nested_array = (c->resp > 2 && count != -1);

    genericZpopCommand(c, &c->argv[1], 1, where, 0, count, use_nested_array, 0, NULL);
}

/* ZPOPMIN key [<count>] */
void zpopminCommand(client *c) {
    zpopMinMaxCommand(c, ZSET_MIN);
}

/* ZPOPMAX key [<count>] */
void zpopmaxCommand(client *c) {
    zpopMinMaxCommand(c, ZSET_MAX);
}

/* BZPOPMIN, BZPOPMAX, BZMPOP actual implementation.
 *
 * 'numkeys' is the number of keys.
 *
 * 'timeout_idx' parameter position of block timeout.
 *
 * 'where' ZSET_MIN or ZSET_MAX.
 *
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * 'use_nested_array' when false it generates a flat array (with or without key name).
 * When true, it generates a nested 3 level array of keyname, field + score pairs.
 * */
void blockingGenericZpopCommand(client *c, robj **keys, int numkeys, int where,
                                int timeout_idx, long count, int use_nested_array, int reply_nil_when_empty) {
    robj *o;
    robj *key;
    mstime_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c,c->argv[timeout_idx],&timeout,UNIT_SECONDS)
        != C_OK) return;

    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db,key);
        /* Non-existing key, move to next key. */
        if (o == NULL) continue;

        if (checkType(c,o,OBJ_ZSET)) return;

        long llen = zsetLength(o);
        /* Empty zset, move to next key. */
        if (llen == 0) continue;

        /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
        genericZpopCommand(c, &key, 1, where, 1, count, use_nested_array, reply_nil_when_empty, NULL);

        if (count == -1) {
            /* Replicate it as ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
            rewriteClientCommandVector(c,2,
                                       (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                       key);
        } else {
            /* Replicate it as ZPOP[MIN|MAX] with COUNT option. */
            robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
            rewriteClientCommandVector(c, 3,
                                       (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                       key, count_obj);
            decrRefCount(count_obj);
        }

        return;
    }

    /* If we are not allowed to block the client and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    blockForKeys(c,BLOCKED_ZSET,keys,numkeys,timeout,0);
}

// BZPOPMIN key [key ...] timeout
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c, c->argv+1, c->argc-2, ZSET_MIN, c->argc-1, -1, 0, 0);
}

// BZPOPMAX key [key ...] timeout
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c, c->argv+1, c->argc-2, ZSET_MAX, c->argc-1, -1, 0, 0);
}

static void zrandmemberReplyWithListpack(client *c, unsigned int count, listpackEntry *keys, listpackEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval) {
                addReplyDouble(c, zzlStrtod(vals[i].sval,vals[i].slen));
            } else
                addReplyDouble(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the zset compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define ZRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
#define ZRANDMEMBER_RANDOM_SAMPLE_LIMIT 1000

void zrandmemberWithCountCommand(client *c, long l, int withscores) {
    unsigned long count, size;
    int uniq = 1;
    robj *zsetobj;

    if ((zsetobj = lookupKeyReadOrReply(c, c->argv[1], shared.emptyarray))
        == NULL || checkType(c, zsetobj, OBJ_ZSET)) return;
    size = zsetLength(zsetobj);

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zsetobj->ptr;
            while (count--) {
                dictEntry *de = dictGetFairRandomKey(zs->dict);
                sds key = dictGetKey(de);
                if (withscores && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withscores)
                    addReplyDouble(c, *(double*)dictGetVal(de));
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
        } else if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            limit = count > ZRANDMEMBER_RANDOM_SAMPLE_LIMIT ? ZRANDMEMBER_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(listpackEntry)*limit);
            if (withscores)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomPairs(zsetobj->ptr, sample_count, keys, vals, 2);
                zrandmemberReplyWithListpack(c, sample_count, keys, vals);
                if (c->flags & CLIENT_CLOSE_ASAP)
                    break;
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    zsetopsrc src;
    zsetopval zval;
    src.subject = zsetobj;
    src.type = zsetobj->type;
    src.encoding = zsetobj->encoding;
    zuiInitIterator(&src);
    memset(&zval, 0, sizeof(zval));

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    long reply_size = count < size ? count : size;
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the zset: simply return the whole zset. */
    if (count >= size) {
        while (zuiNext(&src, &zval)) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, zuiNewSdsFromValue(&zval));
            if (withscores)
                addReplyDouble(c, zval.score);
        }
        zuiClearIterator(&src);
        return;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded zsets are meant to be relatively small, so
     * ZRANDMEMBER_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
        listpackEntry *keys, *vals = NULL;
        keys = zmalloc(sizeof(listpackEntry)*count);
        if (withscores)
            vals = zmalloc(sizeof(listpackEntry)*count);
        serverAssert(lpRandomPairsUnique(zsetobj->ptr, count, keys, vals, 2) == count);
        zrandmemberReplyWithListpack(c, count, keys, vals);
        zfree(keys);
        zfree(vals);
        zuiClearIterator(&src);
        return;
    }

    /* CASE 3:
     * The number of elements inside the zset is not greater than
     * ZRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a dict from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count*ZRANDMEMBER_SUB_STRATEGY_MUL > size) {
        /* Hashtable encoding (generic implementation) */
        dict *d = dictCreate(&sdsReplyDictType);
        dictExpand(d, size);
        /* Add all the elements into the temporary dictionary. */
        while (zuiNext(&src, &zval)) {
            sds key = zuiNewSdsFromValue(&zval);
            dictEntry *de = dictAddRaw(d, key, NULL);
            serverAssert(de);
            if (withscores)
                dictSetDoubleVal(de, zval.score);
        }
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, dictGetKey(de));
            if (withscores)
                addReplyDouble(c, dictGetDoubleVal(de));
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big zset compared to the requested number of elements.
     * In this case we can simply get random elements from the zset and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        /* Hashtable encoding (generic implementation) */
        unsigned long added = 0;
        dict *d = dictCreate(&hashDictType);
        dictExpand(d, count);

        while (added < count) {
            listpackEntry key;
            double score;
            zsetTypeRandomElement(zsetobj, size, &key, withscores ? &score: NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            sds skey = zsetSdsFromListpackEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            added++;

            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            zsetReplyFromListpackEntry(c, &key);
            if (withscores)
                addReplyDouble(c, score);
        }

        /* Release memory */
        dictRelease(d);
    }
    zuiClearIterator(&src);
}

/* ZRANDMEMBER key [<count> [WITHSCORES]] */
void zrandmemberCommand(client *c) {
    long l;
    int withscores = 0;
    robj *zset;
    listpackEntry ele;

    if (c->argc >= 3) {
        if (getRangeLongFromObjectOrReply(c,c->argv[2],-LONG_MAX,LONG_MAX,&l,NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withscores"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4) {
            withscores = 1;
            if (l < -LONG_MAX/2 || l > LONG_MAX/2) {
                addReplyError(c,"value is out of range");
                return;
            }
        }
        zrandmemberWithCountCommand(c, l, withscores);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    if ((zset = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,zset,OBJ_ZSET)) {
        return;
    }

    zsetTypeRandomElement(zset, zsetLength(zset), &ele,NULL);
    zsetReplyFromListpackEntry(c,&ele);
}

/* ZMPOP/BZMPOP
 *
 * 'numkeys_idx' parameter position of key number.
 * 'is_block' this indicates whether it is a blocking variant. */
void zmpopGenericCommand(client *c, int numkeys_idx, int is_block) {
    long j;
    long numkeys = 0;      /* Number of keys. */
    int where = 0;         /* ZSET_MIN or ZSET_MAX. */
    long count = -1;       /* Reply will consist of up to count elements, depending on the zset's length. */

    /* Parse the numkeys. */
    if (getRangeLongFromObjectOrReply(c, c->argv[numkeys_idx], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;

    /* Parse the where. where_idx: the index of where in the c->argv. */
    long where_idx = numkeys_idx + numkeys + 1;
    if (where_idx >= c->argc) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    if (!strcasecmp(c->argv[where_idx]->ptr, "MIN")) {
        where = ZSET_MIN;
    } else if (!strcasecmp(c->argv[where_idx]->ptr, "MAX")) {
        where = ZSET_MAX;
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Parse the optional arguments. */
    for (j = where_idx + 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (count == -1 && !strcasecmp(opt, "COUNT") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], 1, LONG_MAX,
                                              &count,"count should be greater than 0") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (count == -1) count = 1;

    if (is_block) {
        /* BLOCK. We will handle CLIENT_DENY_BLOCKING flag in blockingGenericZpopCommand. */
        blockingGenericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1);
    } else {
        /* NON-BLOCK */
        genericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1, NULL);
    }
}

/* ZMPOP numkeys key [<key> ...] MIN|MAX [COUNT count] */
void zmpopCommand(client *c) {
    zmpopGenericCommand(c, 1, 0);
}

/* BZMPOP timeout numkeys key [<key> ...] MIN|MAX [COUNT count] */
void bzmpopCommand(client *c) {
    zmpopGenericCommand(c, 2, 1);
}
