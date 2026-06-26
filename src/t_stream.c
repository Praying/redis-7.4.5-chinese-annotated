/*
 * Copyright (c) 2017-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "endianconv.h"
#include "stream.h"

/* listpack 内每个 stream 条目都有一个 flags 字段，
 * 用于将条目标记为已删除，或标记该条目的字段与 listpack 开头的 "master" 条目相同。*/
#define STREAM_ITEM_FLAG_NONE 0             /* 没有特殊标记。 */
#define STREAM_ITEM_FLAG_DELETED (1<<0)     /* 条目已删除。迭代时跳过。 */
#define STREAM_ITEM_FLAG_SAMEFIELDS (1<<1)  /* 与 master 条目的字段相同。 */

/* 对于需要多个 ID 的 stream 命令，
 * 当 ID 数量小于 'STREAMID_STATIC_VECTOR_LEN' 时，
 * 避免使用 malloc 动态分配。*/
#define STREAMID_STATIC_VECTOR_LEN 8

/* listpack 的最大预分配大小。这样做是为了避免用户
 * 将 stream_node_max_bytes 设置为过大数值造成的滥用。 */
#define STREAM_LISTPACK_MAX_PRE_ALLOCATE 4096

/* 即使用户配置允许，也不要让 listpack 增长得太大。
 * 否则可能导致溢出（试图将超过 32 位长度的数据存储到 listpack 头中），
 * 或者实际上触发一个断言失败，因为 lpInsert 将返回 NULL。 */
#define STREAM_LISTPACK_MAX_SIZE (1<<30)

void streamFreeCG(streamCG *cg);
void streamFreeNACK(streamNACK *na);
size_t streamReplyWithRangeFromConsumerPEL(client *c, stream *s, streamID *start, streamID *end, size_t count, streamConsumer *consumer);
int streamParseStrictIDOrReply(client *c, robj *o, streamID *id, uint64_t missing_seq, int *seq_given);
int streamParseIDOrReply(client *c, robj *o, streamID *id, uint64_t missing_seq);

/* -----------------------------------------------------------------------
 * 底层 stream 编码：由 listpack 构成的基数树。
 * ----------------------------------------------------------------------- */

/* 创建一个新的 stream 数据结构。*/
stream *streamNew(void) {
    stream *s = zmalloc(sizeof(*s));
    s->rax = raxNew();
    s->length = 0;
    s->first_id.ms = 0;
    s->first_id.seq = 0;
    s->last_id.ms = 0;
    s->last_id.seq = 0;
    s->max_deleted_entry_id.seq = 0;
    s->max_deleted_entry_id.ms = 0;
    s->entries_added = 0;
    s->cgroups = NULL; /* 按需创建，以便在不使用时节省内存。 */
    return s;
}

/* 释放一个 stream，包括存储在基数树中的所有 listpack。*/
void freeStream(stream *s) {
    raxFreeWithCallback(s->rax,(void(*)(void*))lpFree);
    if (s->cgroups)
        raxFreeWithCallback(s->cgroups,(void(*)(void*))streamFreeCG);
    zfree(s);
}

/* 返回 stream 的长度。*/
unsigned long streamLength(const robj *subject) {
    stream *s = subject->ptr;
    return s->length;
}

/* 将 'id' 设置为其后继 stream ID。
 * 如果 'id' 是最大可能的 ID，则环绕回 0-0 并返回 C_ERR。*/
int streamIncrID(streamID *id) {
    int ret = C_OK;
    if (id->seq == UINT64_MAX) {
        if (id->ms == UINT64_MAX) {
            /* 特殊情况：'id' 已经是最后一个可能的 streamID... */
            id->ms = id->seq = 0;
            ret = C_ERR;
        } else {
            id->ms++;
            id->seq = 0;
        }
    } else {
        id->seq++;
    }
    return ret;
}

/* 将 'id' 设置为其前驱 stream ID。
 * 如果 'id' 是最小可能的 ID，则保持 0-0 并返回 C_ERR。*/
int streamDecrID(streamID *id) {
    int ret = C_OK;
    if (id->seq == 0) {
        if (id->ms == 0) {
            /* 特殊情况：'id' 已经是第一个可能的 streamID... */
            id->ms = id->seq = UINT64_MAX;
            ret = C_ERR;
        } else {
            id->ms--;
            id->seq = UINT64_MAX;
        }
    } else {
        id->seq--;
    }
    return ret;
}

/* 根据上一个 stream 条目 ID 生成下一个 ID。如果当前 Unix 毫秒时间
 * 大于上一个 ID 的时间部分，则直接使用该时间作为时间部分，
 * 序列号从 0 开始。否则使用上一个时间（绝不回退）并递增序列号。*/
void streamNextID(streamID *last_id, streamID *new_id) {
    uint64_t ms = commandTimeSnapshot();
    if (ms > last_id->ms) {
        new_id->ms = ms;
        new_id->seq = 0;
    } else {
        *new_id = *last_id;
        streamIncrID(new_id);
    }
}

/* 这是 COPY 命令的辅助函数。
 * 复制一个 Stream 对象，保证返回的对象与原始对象具有相同的编码。
 *
 * 返回对象的 refcount 始终设置为 1 */
robj *streamDup(robj *o) {
    robj *sobj;

    serverAssert(o->type == OBJ_STREAM);

    switch (o->encoding) {
        case OBJ_ENCODING_STREAM:
            sobj = createStreamObject();
            break;
        default:
            serverPanic("Wrong encoding.");
            break;
    }

    stream *s;
    stream *new_s;
    s = o->ptr;
    new_s = sobj->ptr;

    raxIterator ri;
    uint64_t rax_key[2];
    raxStart(&ri, s->rax);
    raxSeek(&ri, "^", NULL, 0);
    size_t lp_bytes = 0;      /* listpack 中的总字节数。 */
    unsigned char *lp = NULL; /* listpack 指针。 */
    /* 获取 listpack 节点的引用。 */
    while (raxNext(&ri)) {
        lp = ri.data;
        lp_bytes = lpBytes(lp);
        unsigned char *new_lp = zmalloc(lp_bytes);
        memcpy(new_lp, lp, lp_bytes);
        memcpy(rax_key, ri.key, sizeof(rax_key));
        raxInsert(new_s->rax, (unsigned char *)&rax_key, sizeof(rax_key),
                  new_lp, NULL);
    }
    new_s->length = s->length;
    new_s->first_id = s->first_id;
    new_s->last_id = s->last_id;
    new_s->max_deleted_entry_id = s->max_deleted_entry_id;
    new_s->entries_added = s->entries_added;
    raxStop(&ri);

    if (s->cgroups == NULL) return sobj;

    /* 复制消费组 */
    raxIterator ri_cgroups;
    raxStart(&ri_cgroups, s->cgroups);
    raxSeek(&ri_cgroups, "^", NULL, 0);
    while (raxNext(&ri_cgroups)) {
        streamCG *cg = ri_cgroups.data;
        streamCG *new_cg = streamCreateCG(new_s, (char *)ri_cgroups.key,
                                          ri_cgroups.key_len, &cg->last_id,
                                          cg->entries_read);

        serverAssert(new_cg != NULL);

        /* 复制消费组的 PEL（待处理消息列表） */
        raxIterator ri_cg_pel;
        raxStart(&ri_cg_pel,cg->pel);
        raxSeek(&ri_cg_pel,"^",NULL,0);
        while(raxNext(&ri_cg_pel)){
            streamNACK *nack = ri_cg_pel.data;
            streamNACK *new_nack = streamCreateNACK(NULL);
            new_nack->delivery_time = nack->delivery_time;
            new_nack->delivery_count = nack->delivery_count;
            raxInsert(new_cg->pel, ri_cg_pel.key, sizeof(streamID), new_nack, NULL);
        }
        raxStop(&ri_cg_pel);

        /* 复制消费者 */
        raxIterator ri_consumers;
        raxStart(&ri_consumers, cg->consumers);
        raxSeek(&ri_consumers, "^", NULL, 0);
        while (raxNext(&ri_consumers)) {
            streamConsumer *consumer = ri_consumers.data;
            streamConsumer *new_consumer;
            new_consumer = zmalloc(sizeof(*new_consumer));
            new_consumer->name = sdsdup(consumer->name);
            new_consumer->pel = raxNew();
            raxInsert(new_cg->consumers,(unsigned char *)new_consumer->name,
                        sdslen(new_consumer->name), new_consumer, NULL);
            new_consumer->seen_time = consumer->seen_time;
            new_consumer->active_time = consumer->active_time;

            /* 复制消费者的 PEL */
            raxIterator ri_cpel;
            raxStart(&ri_cpel, consumer->pel);
            raxSeek(&ri_cpel, "^", NULL, 0);
            while (raxNext(&ri_cpel)) {
                void *result;
                int found = raxFind(new_cg->pel,ri_cpel.key,sizeof(streamID),&result);

                serverAssert(found);

                streamNACK *new_nack = result;
                new_nack->consumer = new_consumer;
                raxInsert(new_consumer->pel,ri_cpel.key,sizeof(streamID),new_nack,NULL);
            }
            raxStop(&ri_cpel);
        }
        raxStop(&ri_consumers);
    }
    raxStop(&ri_cgroups);
    return sobj;
}

/* 这是 lpGet() 的包装函数，用于直接从 listpack（可能将数字以字符串形式存储）中
 * 获取整数值，并在需要时将字符串转换为整数。
 * 'valid' 参数是可选的输出参数，用于指示记录是否有效，
 * 当该参数为 NULL 时，函数将失败并触发断言。*/
static inline int64_t lpGetIntegerIfValid(unsigned char *ele, int *valid) {
    int64_t v;
    unsigned char *e = lpGet(ele,&v,NULL);
    if (e == NULL) {
        if (valid)
            *valid = 1;
        return v;
    }
    /* 按照 listpack 的设计，以下代码路径永远不应被执行：
     * 它们应当总能以整数编码形式存储 int64_t 值。
     * 但实现可能会发生变化。*/
    long long ll;
    int ret = string2ll((char*)e,v,&ll);
    if (valid)
        *valid = ret;
    else
        serverAssert(ret != 0);
    v = ll;
    return v;
}

#define lpGetInteger(ele) lpGetIntegerIfValid(ele, NULL)

/* 获取给定 listpack 边缘条目的 streamID。
 * 'master_id' 是输入参数，用于构造输出参数 'edge_id' */
int lpGetEdgeStreamID(unsigned char *lp, int first, streamID *master_id, streamID *edge_id)
{
   if (lp == NULL)
       return 0;

   unsigned char *lp_ele;

   /* 根据迭代方向，定位到第一个或最后一个条目。*/
   if (first) {
       /* 获取 master 条目的字段数。*/
       lp_ele = lpFirst(lp);        /* 定位到条目计数 */
       lp_ele = lpNext(lp, lp_ele); /* 定位到删除计数 */
       lp_ele = lpNext(lp, lp_ele); /* 定位到字段数 */
       int64_t master_fields_count = lpGetInteger(lp_ele);
       lp_ele = lpNext(lp, lp_ele); /* 定位到第一个字段 */

       /* 若按正序迭代，则跳过 master 字段以定位到第一个实际条目。*/
       for (int64_t i = 0; i < master_fields_count; i++)
           lp_ele = lpNext(lp, lp_ele);

       /* 若正向迭代，跳过前一条目的 lp-count 字段
        * （若是 master 条目，则跳过零终止字段）*/
       lp_ele = lpNext(lp, lp_ele);
       if (lp_ele == NULL)
           return 0;
   } else {
       /* 若按逆序迭代，则直接定位到 listpack 中最后一条目的最后一个部分（即字段计数）。*/
       lp_ele = lpLast(lp);

       /* 若反向迭代，读取当前条目包含的元素个数，然后向前跳 N 次定位到其起始位置。*/
       int64_t lp_count = lpGetInteger(lp_ele);
       if (lp_count == 0) /* 已到达 master 条目。*/
           return 0;

       while (lp_count--)
           lp_ele = lpPrev(lp, lp_ele);
   }

   lp_ele = lpNext(lp, lp_ele); /* 定位到 ID（lp_ele 当前指向 'flags'） */

   /* 获取 ID：ID 被编码为与 master ID 之间的差值。*/
   streamID id = *master_id;
   id.ms += lpGetInteger(lp_ele);
   lp_ele = lpNext(lp, lp_ele);
   id.seq += lpGetInteger(lp_ele);
   *edge_id = id;
   return 1;
}

/* 调试函数，用于输出 listpack 的完整内容。
 * 在开发和调试时很有用。*/
void streamLogListpackContent(unsigned char *lp) {
    unsigned char *p = lpFirst(lp);
    while(p) {
        unsigned char buf[LP_INTBUF_SIZE];
        int64_t v;
        unsigned char *ele = lpGet(p,&v,buf);
        serverLog(LL_WARNING,"- [%d] '%.*s'", (int)v, (int)v, ele);
        p = lpNext(lp,p);
    }
}

/* 将指定的 stream 条目 ID 编码为 128 位大端整数，
 * 以便可以按字典序对 ID 进行排序。*/
void streamEncodeID(void *buf, streamID *id) {
    uint64_t e[2];
    e[0] = htonu64(id->ms);
    e[1] = htonu64(id->seq);
    memcpy(buf,e,sizeof(e));
}

/* 这是 streamEncodeID() 的逆操作：解码后的 ID 将被存放到传入的 'id' 结构中。
 * 缓冲区 'buf' 必须指向一个 128 位大端编码的 ID。*/
void streamDecodeID(void *buf, streamID *id) {
    uint64_t e[2];
    memcpy(e,buf,sizeof(e));
    id->ms = ntohu64(e[0]);
    id->seq = ntohu64(e[1]);
}

/* 比较两个 stream ID。若 a < b 返回 -1，若 a == b 返回 0，若 a > b 返回 1。*/
int streamCompareID(streamID *a, streamID *b) {
    if (a->ms > b->ms) return 1;
    else if (a->ms < b->ms) return -1;
    /* ms 部分相同，比较序列号部分。*/
    else if (a->seq > b->seq) return 1;
    else if (a->seq < b->seq) return -1;
    /* 全部相同：两个 ID 相等。*/
    return 0;
}

/* 获取 stream 边缘条目的 ID。边缘可以是流中的第一个或最后一个 ID，
 * 也可能是一个墓碑条目（即已删除条目）。若要过滤掉墓碑条目，
 * 可将 'skip_tombstones' 参数设置为 1。*/
void streamGetEdgeID(stream *s, int first, int skip_tombstones, streamID *edge_id)
{
    streamIterator si;
    int64_t numfields;
    streamIteratorStart(&si,s,NULL,NULL,!first);
    si.skip_tombstones = skip_tombstones;
    int found = streamIteratorGetID(&si,edge_id,&numfields);
    if (!found) {
        streamID min_id = {0, 0}, max_id = {UINT64_MAX, UINT64_MAX};
        *edge_id = first ? max_id : min_id;
    }
    streamIteratorStop(&si);
}

/* 向 stream 's' 中添加一个新条目，其字段值对数量由 'numfields' 指定，
 * 数据存放在 'argv' 中。
 * 通过 'added_id' 结构返回新条目的 ID。
 *
 * 如果 'use_id' 不为 NULL，则不会由函数自动生成 ID，
 * 而是使用传入的 ID 来添加新条目。这种情况下添加条目可能失败，
 * 后续注释中有详细说明。
 *
 * 当 'use_id' 与零 'seq-given' 一起使用时，传入 ID 的序列号部分将被忽略，
 * 函数将尝试使用自动生成的序列号。
 *
 * 如果条目成功添加，函数返回 C_OK，如果 ID 是由函数自动生成的，则必然返回 C_OK。
 * 但在以下几种情况下，函数可能返回 C_ERR：
 * 1. 通过 'use_id' 给定了一个 ID，但由于当前顶部 ID 大于或等于该 ID，
 *    添加失败。errno 将被设置为 EDOM。
 * 2. 如果单个元素的大小或所有元素的合计大小过大，无法存储到 stream 中。
 *    errno 将被设置为 ERANGE。*/
int streamAppendItem(stream *s, robj **argv, int64_t numfields, streamID *added_id, streamID *use_id, int seq_given) {

    /* 生成新条目的 ID。*/
    streamID id;
    if (use_id) {
        if (seq_given) {
            id = *use_id;
        } else {
            /* 自动生成的序列号可以是零（新时间戳）或最后一个 ID 的递增序列号。
             * 在后一种情况下，我们需要防止序列号溢出或在时间上前移。*/
            if (s->last_id.ms == use_id->ms) {
                if (s->last_id.seq == UINT64_MAX) {
                    errno = EDOM;
                    return C_ERR;
                }
                id = s->last_id;
                id.seq++;
            } else {
                id = *use_id;
            }
        }
    } else {
        streamNextID(&s->last_id,&id);
    }

    /* 检查新 ID 必须大于最后一个条目 ID，否则返回错误。
     * 自动生成的 ID 在递增序列号部分时可能溢出（并环绕）。*/
    if (streamCompareID(&id,&s->last_id) <= 0) {
        errno = EDOM;
        return C_ERR;
    }

    /* 避免在向 stream 添加元素时发生溢出（listpack 最多只能容纳 32 位长度的字符串，
     * 且 listpack 的总大小也不能超过 32 位长度）。*/
    size_t totelelen = 0;
    for (int64_t i = 0; i < numfields*2; i++) {
        sds ele = argv[i]->ptr;
        totelelen += sdslen(ele);
    }
    if (totelelen > STREAM_LISTPACK_MAX_SIZE) {
        errno = ERANGE;
        return C_ERR;
    }

    /* 添加新条目。*/
    raxIterator ri;
    raxStart(&ri,s->rax);
    raxSeek(&ri,"$",NULL,0);

    size_t lp_bytes = 0;        /* 尾 listpack 中的总字节数。*/
    unsigned char *lp = NULL;   /* 尾 listpack 指针。*/

    if (!raxEOF(&ri)) {
        /* 获取尾部节点 listpack 的引用。*/
        lp = ri.data;
        lp_bytes = lpBytes(lp);
    }
    raxStop(&ri);

    /* 我们必须按字典序将键添加到基数树中，
     * 为此，我们将 ID 视为一个以大端序写入的 128 位整数，
     * 这样最高有效字节排在最前面。*/
    uint64_t rax_key[2];    /* 基数树中包含 listpack 的键。*/
    streamID master_id;     /* listpack 中 master 条目的 ID。*/

    /* 如果需要，创建新的 listpack 和基数树节点。请注意，当创建新的 listpack 时，
     * 我们会用 "master 条目" 来填充它。这只是一组字段，作为后续添加到
     * listpack 中的 stream 条目的参考，用于压缩。
     *
     * 注意，虽然我们使用第一个添加的条目的字段来创建 master 条目，
     * 但第一个添加的条目本身并不表示在 master 条目中，master 条目是一个独立的对象。
     * 当然，第一个条目本身将得到很好的压缩，因为它被用作参考。
     *
     * master 条目的组成如下例所示：
     *
     * +-------+---------+------------+---------+--/--+---------+---------+-+
     * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
     * +-------+---------+------------+---------+--/--+---------+---------+-+
     *
     * count 和 deleted 分别表示 listpack 内有效条目的总数和被标记为已删除
     * （在条目 flags 中设置了 deleted 标记）的条目数。因此 listpack 中
     * 实际存在的条目总数（已删除和未删除的合计）为 count+deleted。
     *
     * 真正的条目将使用与存储在包含该 listpack 的基数树节点处的 key 之间的
     * 毫秒和序列号差值来编码 ID（差分编码），如果该条目的字段与 master
     * 条目的字段相同，则条目的 flags 将指定这一事实，并且条目的字段和字段数
     * 将被省略（参见本函数后续代码）。
     *
     * 末尾的 "0" 条目与常规 stream 条目中的 'lp-count' 条目相同
     * （见下文），它标记当从右向左扫描 stream 时不再有更多条目。*/

    /* 首先，检查是否可以追加到当前宏节点，或者是否需要切换到下一个节点。
     * 如果当前节点已满，则将 'lp' 设置为 NULL。*/
    if (lp != NULL) {
        int new_node = 0;
        size_t node_max_bytes = server.stream_node_max_bytes;
        if (node_max_bytes == 0 || node_max_bytes > STREAM_LISTPACK_MAX_SIZE)
            node_max_bytes = STREAM_LISTPACK_MAX_SIZE;
        if (lp_bytes + totelelen >= node_max_bytes) {
            new_node = 1;
        } else if (server.stream_node_max_entries) {
            unsigned char *lp_ele = lpFirst(lp);
            /* 同时统计有效条目和已删除条目。*/
            int64_t count = lpGetInteger(lp_ele) + lpGetInteger(lpNext(lp,lp_ele));
            if (count >= server.stream_node_max_entries) new_node = 1;
        }

        if (new_node) {
            /* 收缩额外预分配的内存 */
            lp = lpShrinkToFit(lp);
            if (ri.data != lp)
                raxInsert(s->rax,ri.key,ri.key_len,lp,NULL);
            lp = NULL;
        }
    }

    int flags = STREAM_ITEM_FLAG_NONE;
    if (lp == NULL) {
        master_id = id;
        streamEncodeID(rax_key,&id);
        /* 创建包含 master 条目 ID 和字段的 listpack。
         * 创建 listpack 时预分配一些字节，以避免每次 XADD 都进行 realloc。
         * 由于 listpack.c 使用 malloc_size，它将按步长增长，
         * 不会在每次 XADD 时都进行 realloc。
         * 当 listpack 达到最大条目数时，我们会收缩内存分配以适配实际数据。*/
        size_t prealloc = STREAM_LISTPACK_MAX_PRE_ALLOCATE;
        if (server.stream_node_max_bytes > 0 && server.stream_node_max_bytes < prealloc) {
            prealloc = server.stream_node_max_bytes;
        }
        lp = lpNew(prealloc);
        lp = lpAppendInteger(lp,1); /* 一个条目，即我们正在添加的条目。*/
        lp = lpAppendInteger(lp,0); /* 当前已删除条目数为 0。*/
        lp = lpAppendInteger(lp,numfields);
        for (int64_t i = 0; i < numfields; i++) {
            sds field = argv[i*2]->ptr;
            lp = lpAppend(lp,(unsigned char*)field,sdslen(field));
        }
        lp = lpAppendInteger(lp,0); /* master 条目的零终止符。*/
        raxInsert(s->rax,(unsigned char*)&rax_key,sizeof(rax_key),lp,NULL);
        /* 我们插入的第一个条目显然具有与 master 条目相同的字段。*/
        flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
    } else {
        serverAssert(ri.key_len == sizeof(rax_key));
        memcpy(rax_key,ri.key,sizeof(rax_key));

        /* 从基数树 key 中读取 master ID。*/
        streamDecodeID(rax_key,&master_id);
        unsigned char *lp_ele = lpFirst(lp);

        /* 更新 count 字段并跳过 deleted 字段。*/
        int64_t count = lpGetInteger(lp_ele);
        lp = lpReplaceInteger(lp,&lp_ele,count+1);
        lp_ele = lpNext(lp,lp_ele); /* 定位到 deleted。*/
        lp_ele = lpNext(lp,lp_ele); /* 定位到 master 条目的字段数。*/

        /* 检查我们正在添加的条目是否与 master 条目具有相同的字段。*/
        int64_t master_fields_count = lpGetInteger(lp_ele);
        lp_ele = lpNext(lp,lp_ele);
        if (numfields == master_fields_count) {
            int64_t i;
            for (i = 0; i < master_fields_count; i++) {
                sds field = argv[i*2]->ptr;
                int64_t e_len;
                unsigned char buf[LP_INTBUF_SIZE];
                unsigned char *e = lpGet(lp_ele,&e_len,buf);
                /* 若出现不匹配则停止。*/
                if (sdslen(field) != (size_t)e_len ||
                    memcmp(e,field,e_len) != 0) break;
                lp_ele = lpNext(lp,lp_ele);
            }
            /* 所有字段都相同！我们可以通过在 flags 中设置一个位来压缩字段名。*/
            if (i == master_fields_count) flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
        }
    }

    /* 用新条目填充 listpack。我们使用以下编码：
     *
     * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
     * |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
     * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
     *
     * 但是，如果设置了 SAMEFIELD 标志，我们只需用值填充条目即可，
     * 因此它将变为：
     *
     * +-----+--------+-------+-/-+-------+--------+
     * |flags|entry-id|value-1|...|value-N|lp-count|
     * +-----+--------+-------+-/-+-------+--------+
     *
     * entry-id 字段实际上由两个独立的字段组成：与 master 条目相比的
     * 毫秒和序列号差值。
     *
     * lp-count 字段是一个数字，表示组成该条目的 listpack 片段数，
     * 以便可以反向遍历条目：我们可以从 listpack 末尾开始，读取条目，
     * 并向前跳 N 次以定位 "flags" 字段来读取完整的 stream 条目。*/
    lp = lpAppendInteger(lp,flags);
    lp = lpAppendInteger(lp,id.ms - master_id.ms);
    lp = lpAppendInteger(lp,id.seq - master_id.seq);
    if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS))
        lp = lpAppendInteger(lp,numfields);
    for (int64_t i = 0; i < numfields; i++) {
        sds field = argv[i*2]->ptr, value = argv[i*2+1]->ptr;
        if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS))
            lp = lpAppend(lp,(unsigned char*)field,sdslen(field));
        lp = lpAppend(lp,(unsigned char*)value,sdslen(value));
    }
    /* 计算并存储 lp-count 字段。*/
    int64_t lp_count = numfields;
    lp_count += 3; /* 加上 3 个固定字段 flags + ms-diff + seq-diff。*/
    if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) {
        /* 如果条目未被压缩，它还包含字段名（除了值之外），
         * 以及一个额外的 num-fields 字段。*/
        lp_count += numfields+1;
    }
    lp = lpAppendInteger(lp,lp_count);

    /* 重新插入树中以更新 listpack 指针。*/
    if (ri.data != lp)
        raxInsert(s->rax,(unsigned char*)&rax_key,sizeof(rax_key),lp,NULL);
    s->length++;
    s->entries_added++;
    s->last_id = id;
    if (s->length == 1) s->first_id = id;
    if (added_id) *added_id = id;
    return C_OK;
}

typedef struct {
    /* XADD 选项 */
    streamID id; /* 用户提供的 ID，仅用于 XADD。*/
    int id_given; /* 是否指定了不同于 "*" 的 ID？仅用于 XADD。*/
    int seq_given; /* 是否指定了不同于 "ms-*" 的 ID？仅用于 XADD。*/
    int no_mkstream; /* 若设置为 1，则不创建新的 stream */

    /* XADD + XTRIM 通用选项 */
    int trim_strategy; /* TRIM_STRATEGY_* */
    int trim_strategy_arg_idx; /* MAXLEN/MINID 中 count 在参数列表中的索引，用于重写。*/
    int approx_trim; /* 若为 1，则仅删除整个基数树节点，
                      * 因此 trim 参数不会被严格执行。*/
    long long limit; /* 要 trim 的最大条目数。若为 0，则不限制
                      * trim 操作的工作量。*/
    /* TRIM_STRATEGY_MAXLEN 选项 */
    long long maxlen; /* trim 后，stream 保留的长度。*/
    /* TRIM_STRATEGY_MINID 选项 */
    streamID minid; /* 按 ID 修剪（stream 中不会有 ID < 'minid' 的条目保留） */
} streamAddTrimArgs;

#define TRIM_STRATEGY_NONE 0
#define TRIM_STRATEGY_MAXLEN 1
#define TRIM_STRATEGY_MINID 2

/* 根据 args->trim_strategy 修剪 stream 's'，并返回从 stream 中移除的元素数。
 * 若 'approx' 选项非零，则指定修剪必须以近似方式执行，以最大化性能。
 * 这意味着 stream 中可能仍保留 ID < 'id' 的条目（对于 MINID），
 * 或者元素个数可能超过 'maxlen'（对于 MAXLEN），
 * 只有当我们能删除基数树的*整个*节点时，才会删除元素。
 * 元素从 stream 的头部（较旧的元素）开始删除。
 *
 * 该函数在以下情况下可能返回 0：
 *
 * 1) stream 的最小条目 ID 已经 < 'id'（MINID）；或者
 * 2) stream 的长度已经小于或等于指定的最大长度（MAXLEN）；或者
 * 3) 'approx' 选项为 true，但头节点没有足够的元素可删除。
 *
 * args->limit 是要删除的最大条目数。其目的是防止该函数耗时过长。
 * 如果 'limit' 为 0，则不限制被删除的条目数。
 * 与 'approx' 类似，如果 'limit' 小于应修剪的条目数，
 * 则 stream 中仍可能存在 ID < 'id' 的条目（MAXLEN 时元素数可能 >= maxlen）。
 */
int64_t streamTrim(stream *s, streamAddTrimArgs *args) {
    size_t maxlen = args->maxlen;
    streamID *id = &args->minid;
    int approx = args->approx_trim;
    int64_t limit = args->limit;
    int trim_strategy = args->trim_strategy;

    if (trim_strategy == TRIM_STRATEGY_NONE)
        return 0;

    raxIterator ri;
    raxStart(&ri,s->rax);
    raxSeek(&ri,"^",NULL,0);

    int64_t deleted = 0;
    while (raxNext(&ri)) {
        if (trim_strategy == TRIM_STRATEGY_MAXLEN && s->length <= maxlen)
            break;

        unsigned char *lp = ri.data, *p = lpFirst(lp);
        int64_t entries = lpGetInteger(p);

        /* 检查我们是否超出了可执行的工作量 */
        if (limit && (deleted + entries) > limit)
            break;

        /* 检查是否可以删除整个节点。*/
        int remove_node;
        streamID master_id = {0}; /* 用于 MINID */
        if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
            remove_node = s->length - entries >= maxlen;
        } else {
            /* 从基数树 key 中读取 master ID。*/
            streamDecodeID(ri.key, &master_id);

            /* 读取最后一个 ID。*/
            streamID last_id = {0,0};
            lpGetEdgeStreamID(lp, 0, &master_id, &last_id);

            /* 如果节点的最后一个 ID < 'id'，则可以删除整个节点 */
            remove_node = streamCompareID(&last_id, id) < 0;
        }

        if (remove_node) {
            lpFree(lp);
            raxRemove(s->rax,ri.key,ri.key_len,NULL);
            raxSeek(&ri,">=",ri.key,ri.key_len);
            s->length -= entries;
            deleted += entries;
            continue;
        }

        /* 如果无法删除整个元素，且 approx 为 true，则在此停止。*/
        if (approx) break;

        /* 现在我们需要从 'lp' 内部修剪条目 */
        int64_t deleted_from_lp = 0;

        p = lpNext(lp, p); /* 跳过 deleted 字段。*/
        p = lpNext(lp, p); /* 跳过 master 条目中的字段数。*/

        /* 跳过所有 master 字段。*/
        int64_t master_fields_count = lpGetInteger(p);
        p = lpNext(lp,p); /* 跳过第一个字段。*/
        for (int64_t j = 0; j < master_fields_count; j++)
            p = lpNext(lp,p); /* 跳过所有 master 字段。*/
        p = lpNext(lp,p); /* 跳过 master 条目的零终止符。*/

        /* 'p' 现在指向 listpack 中的第一个条目。
         * 我们需要逐条扫描条目，将未删除的条目标记为已删除。*/
        while (p) {
            /* 我们保留一份 p（指向 flags 部分）的副本，
             * 以便在（且仅当）实际移除条目后更新它 */
            unsigned char *pcopy = p;

            int64_t flags = lpGetInteger(p);
            p = lpNext(lp, p); /* 跳过 flags。*/
            int64_t to_skip;

            int64_t ms_delta = lpGetInteger(p);
            p = lpNext(lp, p); /* 跳过 ID ms 差值 */
            int64_t seq_delta = lpGetInteger(p);
            p = lpNext(lp, p); /* 跳过 ID seq 差值 */

            streamID currid = {0}; /* 用于 MINID */
            if (trim_strategy == TRIM_STRATEGY_MINID) {
                currid.ms = master_id.ms + ms_delta;
                currid.seq = master_id.seq + seq_delta;
            }

            int stop;
            if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
                stop = s->length <= maxlen;
            } else {
                /* 由于 rax 树是有序的，后续 ID 必然更大，没有必要继续。*/
                stop = streamCompareID(&currid, id) >= 0;
            }
            if (stop)
                break;

            if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
                to_skip = master_fields_count;
            } else {
                to_skip = lpGetInteger(p); /* 获取字段数。*/
                p = lpNext(lp,p); /* 跳过字段数。*/
                to_skip *= 2; /* 字段和值。*/
            }

            while(to_skip--) p = lpNext(lp,p); /* 跳过整个条目。*/
            p = lpNext(lp,p); /* 跳过末尾的 lp-count 字段。*/

            /* 将条目标记为已删除。*/
            if (!(flags & STREAM_ITEM_FLAG_DELETED)) {
                intptr_t delta = p - lp;
                flags |= STREAM_ITEM_FLAG_DELETED;
                lp = lpReplaceInteger(lp, &pcopy, flags);
                deleted_from_lp++;
                s->length--;
                p = lp + delta;
            }
        }
        deleted += deleted_from_lp;

        /* 现在我们更新条目/已删除计数器。*/
        p = lpFirst(lp);
        lp = lpReplaceInteger(lp,&p,entries-deleted_from_lp);
        p = lpNext(lp,p); /* 跳过 deleted 字段。*/
        int64_t marked_deleted = lpGetInteger(p);
        lp = lpReplaceInteger(lp,&p,marked_deleted+deleted_from_lp);
        p = lpNext(lp,p); /* 跳过 master 条目中的字段数。*/

        /* 如果此时 listpack 中已删除条目过多，我们应该执行垃圾回收。*/
        entries -= deleted_from_lp;
        marked_deleted += deleted_from_lp;
        if (entries + marked_deleted > 10 && marked_deleted > entries/2) {
            /* TODO: 执行垃圾回收。*/
        }

        /* 用新的 listpack 指针更新树。*/
        raxInsert(s->rax,ri.key,ri.key_len,lp,NULL);

        break; /* 如果到达这里，说明当前节点中已有足够多可删除的条目，
                  不需要继续到下一个节点。*/
    }
    raxStop(&ri);

    /* 在修剪后更新 stream 的 first ID。*/
    if (s->length == 0) {
        s->first_id.ms = 0;
        s->first_id.seq = 0;
    } else if (deleted) {
        streamGetEdgeID(s,1,1,&s->first_id);
    }

    return deleted;
}

/* 按长度修剪 stream。返回已删除的条目数。*/
int64_t streamTrimByLength(stream *s, long long maxlen, int approx) {
    streamAddTrimArgs args = {
        .trim_strategy = TRIM_STRATEGY_MAXLEN,
        .approx_trim = approx,
        .limit = approx ? 100 * server.stream_node_max_entries : 0,
        .maxlen = maxlen
    };
    return streamTrim(s, &args);
}

/* 按最小 ID 修剪 stream。返回已删除的条目数。*/
int64_t streamTrimByID(stream *s, streamID minid, int approx) {
    streamAddTrimArgs args = {
        .trim_strategy = TRIM_STRATEGY_MINID,
        .approx_trim = approx,
        .limit = approx ? 100 * server.stream_node_max_entries : 0,
        .minid = minid
    };
    return streamTrim(s, &args);
}

/* 解析 XADD/XTRIM 的参数。
 *
 * 有关所处理参数的更多详细信息，请参阅 streamAddTrimArgs。
 *
 * 此函数返回 ID 参数的位置（仅与 XADD 相关）。
 * 出错时返回 -1 并向客户端发送错误回复。*/
static int streamParseAddOrTrimArgsOrReply(client *c, streamAddTrimArgs *args, int xadd) {
    /* 将参数初始化为默认值 */
    memset(args, 0, sizeof(*args));

    /* 解析选项。*/
    int i = 2; /* 这是可能找到选项或 ID 的第一个参数位置。*/
    int limit_given = 0;
    for (; i < c->argc; i++) {
        int moreargs = (c->argc-1) - i; /* 剩余参数个数。*/
        char *opt = c->argv[i]->ptr;
        if (xadd && opt[0] == '*' && opt[1] == '\0') {
            /* 这是自动创建 ID 的常见情况的快速路径。*/
            break;
        } else if (!strcasecmp(opt,"maxlen") && moreargs) {
            if (args->trim_strategy != TRIM_STRATEGY_NONE) {
                addReplyError(c,"语法错误，MAXLEN 和 MINID 选项不能同时使用");
                return -1;
            }
            args->approx_trim = 0;
            char *next = c->argv[i+1]->ptr;
            /* 检查 MAXLEN ~ <count> 形式。*/
            if (moreargs >= 2 && next[0] == '~' && next[1] == '\0') {
                args->approx_trim = 1;
                i++;
            } else if (moreargs >= 2 && next[0] == '=' && next[1] == '\0') {
                i++;
            }
            if (getLongLongFromObjectOrReply(c,c->argv[i+1],&args->maxlen,NULL)
                != C_OK) return -1;

            if (args->maxlen < 0) {
                addReplyError(c,"MAXLEN 参数必须 >= 0。");
                return -1;
            }
            i++;
            args->trim_strategy = TRIM_STRATEGY_MAXLEN;
            args->trim_strategy_arg_idx = i;
        } else if (!strcasecmp(opt,"minid") && moreargs) {
            if (args->trim_strategy != TRIM_STRATEGY_NONE) {
                addReplyError(c,"语法错误，MAXLEN 和 MINID 选项不能同时使用");
                return -1;
            }
            args->approx_trim = 0;
            char *next = c->argv[i+1]->ptr;
            /* 检查 MINID ~ <id> 形式 */
            if (moreargs >= 2 && next[0] == '~' && next[1] == '\0') {
                args->approx_trim = 1;
                i++;
            } else if (moreargs >= 2 && next[0] == '=' && next[1] == '\0') {
                i++;
            }

            if (streamParseStrictIDOrReply(c,c->argv[i+1],&args->minid,0,NULL) != C_OK)
                return -1;

            i++;
            args->trim_strategy = TRIM_STRATEGY_MINID;
            args->trim_strategy_arg_idx = i;
        } else if (!strcasecmp(opt,"limit") && moreargs) {
            /* 关于 LIMIT 的说明：如果调用者没有提供，我们将其设置为
             * 100*server.stream_node_max_entries，这是为了防止修剪时间过长，
             * 代价是可能不会删除本应被修剪的条目。
             * 如果用户想要精确修剪（即不使用 '~'），我们不会限制被修剪的条目数。*/
            if (getLongLongFromObjectOrReply(c,c->argv[i+1],&args->limit,NULL) != C_OK)
                return -1;

            if (args->limit < 0) {
                addReplyError(c,"LIMIT 参数必须 >= 0。");
                return -1;
            }
            limit_given = 1;
            i++;
        } else if (xadd && !strcasecmp(opt,"nomkstream")) {
            args->no_mkstream = 1;
        } else if (xadd) {
            /* 如果到达这里，要么是语法错误，要么是有效的 ID。*/
            if (streamParseStrictIDOrReply(c,c->argv[i],&args->id,0,&args->seq_given) != C_OK)
                return -1;
            args->id_given = 1;
            break;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return -1;
        }
    }

    if (args->limit && args->trim_strategy == TRIM_STRATEGY_NONE) {
        addReplyError(c,"语法错误，LIMIT 必须与修剪策略一起使用");
        return -1;
    }

    if (!xadd && args->trim_strategy == TRIM_STRATEGY_NONE) {
        addReplyError(c,"语法错误，XTRIM 必须指定修剪策略");
        return -1;
    }

    if (mustObeyClient(c)) {
        /* 如果命令来自 master 或 AOF，我们不能强制执行 maxnodes
         *（maxlen/minid 参数已被重写以确保没有不一致）。*/
        args->limit = 0;
    } else {
        /* 我们需要设置 limit（仅当我们使用了 '~' 时）*/
        if (limit_given) {
            if (!args->approx_trim) {
                /* 在没有 ~ 的情况下提供了 LIMIT */
                addReplyError(c,"语法错误，LIMIT 必须与特殊的 ~ 选项一起使用");
                return -1;
            }
        } else {
            /* 用户没有提供 LIMIT，我们必须设置它。*/
            if (args->approx_trim) {
                /* 为了防止修剪做太多工作而导致延迟尖峰，我们限制其工作量。
                 * 我们必须从两端限制 args->limit，以防
                 * stream_node_max_entries 为 0 或过大（可能导致溢出）。
                 */
                args->limit = 100 * server.stream_node_max_entries; /* 最多 100 个 rax 节点。*/
                if (args->limit <= 0) args->limit = 10000;
                if (args->limit > 1000000) args->limit = 1000000;
            } else {
                /* 精确修剪不使用 LIMIT */
                args->limit = 0;
            }
        }
    }

    return i;
}

/* 初始化 stream 迭代器，以便我们可以调用迭代函数获取下一个条目。
 * 这需要在结束时调用对应的 streamIteratorStop()。
 * 'rev' 参数控制迭代方向。如果为 0，则从开始到结束（含）进行迭代；
 * 否则如果 rev 非零，则按相反方向进行迭代。
 *
 * 一旦初始化迭代器，我们按如下方式迭代：
 *
 *  streamIterator myiterator;
 *  streamIteratorStart(&myiterator,...);
 *  int64_t numfields;
 *  while(streamIteratorGetID(&myiterator,&ID,&numfields)) {
 *      while(numfields--) {
 *          unsigned char *key, *value;
 *          size_t key_len, value_len;
 *          streamIteratorGetField(&myiterator,&key,&value,&key_len,&value_len);
 *
 *          ... 对 key 和 value 进行所需处理 ...
 *      }
 *  }
 *  streamIteratorStop(&myiterator); */
void streamIteratorStart(streamIterator *si, stream *s, streamID *start, streamID *end, int rev) {
    /* 初始化迭代器，并将迭代的开始/结束元素转换为 128 位大端整数。*/
    if (start) {
        streamEncodeID(si->start_key,start);
    } else {
        si->start_key[0] = 0;
        si->start_key[1] = 0;
    }

    if (end) {
        streamEncodeID(si->end_key,end);
    } else {
        si->end_key[0] = UINT64_MAX;
        si->end_key[1] = UINT64_MAX;
    }

    /* 在基数树中定位到正确的节点。*/
    raxStart(&si->ri,s->rax);
    if (!rev) {
        if (start && (start->ms || start->seq)) {
            raxSeek(&si->ri,"<=",(unsigned char*)si->start_key,
                    sizeof(si->start_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri,"^",NULL,0);
        } else {
            raxSeek(&si->ri,"^",NULL,0);
        }
    } else {
        if (end && (end->ms || end->seq)) {
            raxSeek(&si->ri,"<=",(unsigned char*)si->end_key,
                    sizeof(si->end_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri,"$",NULL,0);
        } else {
            raxSeek(&si->ri,"$",NULL,0);
        }
    }
    si->stream = s;
    si->lp = NULL;     /* 当前没有 listpack。*/
    si->lp_ele = NULL; /* 当前的 listpack 游标。*/
    si->rev = rev;     /* 方向，非零表示反向，从结尾到开始。*/
    si->skip_tombstones = 1;    /* 默认不输出墓碑条目。*/
}

/* 如果迭代范围内还有剩余元素，则返回 1 并将当前条目的 ID 存放在 'id' 中；
 * 否则返回 0 以表示迭代已结束。*/
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields) {
    while(1) { /* 当元素 > stop_key 或到达基数树末尾时停止。*/
        /* 如果当前 listpack 为 NULL，则表示迭代刚开始，或上一个 listpack
         * 已完全迭代完毕。转到下一个节点。*/
        if (si->lp == NULL || si->lp_ele == NULL) {
            if (!si->rev && !raxNext(&si->ri)) return 0;
            else if (si->rev && !raxPrev(&si->ri)) return 0;
            serverAssert(si->ri.key_len == sizeof(streamID));
            /* 获取 master ID。*/
            streamDecodeID(si->ri.key,&si->master_id);
            /* 获取 master 条目的字段数。*/
            si->lp = si->ri.data;
            si->lp_ele = lpFirst(si->lp);           /* 定位到条目计数 */
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* 定位到 deleted 计数 */
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* 定位到 num fields */
            si->master_fields_count = lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* 定位到第一个字段 */
            si->master_fields_start = si->lp_ele;
            /* 我们现在指向 master 条目的第一个字段。
             * 需要根据迭代方向定位到第一个或最后一个条目。*/
            if (!si->rev) {
                /* 如果按正序迭代，跳过 master 字段以定位到第一个实际条目。*/
                for (uint64_t i = 0; i < si->master_fields_count; i++)
                    si->lp_ele = lpNext(si->lp,si->lp_ele);
            } else {
                /* 如果按反序迭代，则直接定位到 listpack 中最后一条目的最后一个部分（即字段计数）。*/
                si->lp_ele = lpLast(si->lp);
            }
        } else if (si->rev) {
            /* 如果按反序迭代，且这不是该 listpack 输出的第一个条目，
             * 那么我们已经输出了当前条目，需要回到前一个条目。*/
            int64_t lp_count = lpGetInteger(si->lp_ele);
            while(lp_count--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
            /* 定位到前一条目的 lp-count 字段。*/
            si->lp_ele = lpPrev(si->lp,si->lp_ele);
        }

        /* 对于每个基数树节点，迭代其对应的 listpack，
         * 当元素在范围内时将其返回。*/
        while(1) {
            if (!si->rev) {
                /* 如果正向迭代，跳过前一条目的 lp-count 字段
                 * （若是 master 条目，则跳过零终止字段）*/
                si->lp_ele = lpNext(si->lp,si->lp_ele);
                if (si->lp_ele == NULL) break;
            } else {
                /* 如果反向迭代，读取当前条目包含的元素个数，并向前跳 N 次定位到其起始位置。*/
                int64_t lp_count = lpGetInteger(si->lp_ele);
                if (lp_count == 0) { /* 已到达 master 条目。*/
                    si->lp = NULL;
                    si->lp_ele = NULL;
                    break;
                }
                while(lp_count--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
            }

            /* 获取 flags 条目。*/
            si->lp_flags = si->lp_ele;
            int64_t flags = lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* 定位到 ID。*/

            /* 获取 ID：ID 被编码为 master ID 与该条目 ID 之间的差值。*/
            *id = si->master_id;
            id->ms += lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele);
            id->seq += lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele);
            unsigned char buf[sizeof(streamID)];
            streamEncodeID(buf,id);

            /* 字段数是否存在取决于 flags。*/
            if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
                *numfields = si->master_fields_count;
            } else {
                *numfields = lpGetInteger(si->lp_ele);
                si->lp_ele = lpNext(si->lp,si->lp_ele);
            }
            serverAssert(*numfields>=0);

            /* 如果当前 ID >= 起始 ID，且条目未被标记为已删除
             * （或允许输出墓碑条目），则输出它。*/
            if (!si->rev) {
                if (memcmp(buf,si->start_key,sizeof(streamID)) >= 0 &&
                    (!si->skip_tombstones || !(flags & STREAM_ITEM_FLAG_DELETED)))
                {
                    if (memcmp(buf,si->end_key,sizeof(streamID)) > 0)
                        return 0; /* 已超出范围。*/
                    si->entry_flags = flags;
                    if (flags & STREAM_ITEM_FLAG_SAMEFIELDS)
                        si->master_fields_ptr = si->master_fields_start;
                    return 1; /* 返回有效条目。*/
                }
            } else {
                if (memcmp(buf,si->end_key,sizeof(streamID)) <= 0 &&
                    (!si->skip_tombstones || !(flags & STREAM_ITEM_FLAG_DELETED)))
                {
                    if (memcmp(buf,si->start_key,sizeof(streamID)) < 0)
                        return 0; /* 已超出范围。*/
                    si->entry_flags = flags;
                    if (flags & STREAM_ITEM_FLAG_SAMEFIELDS)
                        si->master_fields_ptr = si->master_fields_start;
                    return 1; /* 返回有效条目。*/
                }
            }

            /* 如果不输出该条目，我们需要正向丢弃它，或反向定位到前一条目。*/
            if (!si->rev) {
                int64_t to_discard = (flags & STREAM_ITEM_FLAG_SAMEFIELDS) ?
                                      *numfields : *numfields*2;
                for (int64_t i = 0; i < to_discard; i++)
                    si->lp_ele = lpNext(si->lp,si->lp_ele);
            } else {
                int64_t prev_times = 4; /* flag + id ms + id seq + 一次额外跳转
                                           以回到前一条目的 "count" 字段。*/
                /* 如果条目未标记 SAMEFIELD，我们还会读取字段数，因此再回退一次。*/
                if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) prev_times++;
                while(prev_times--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
            }
        }

        /* 当前 listpack 结束，尝试下一个/上一个基数树节点。*/
    }
}

/* 获取当前迭代条目的字段和值。应在 streamIteratorGetID() 之后立即调用，
 * 并按照 streamIteratorGetID() 返回的字段数对每个字段调用一次。
 * 该函数通过引用填充字段和值指针及对应的长度，这些值在下次迭代器调用之前有效，
 * 前提是期间没有其他代码修改 stream。*/
void streamIteratorGetField(streamIterator *si, unsigned char **fieldptr, unsigned char **valueptr, int64_t *fieldlen, int64_t *valuelen) {
    if (si->entry_flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
        *fieldptr = lpGet(si->master_fields_ptr,fieldlen,si->field_buf);
        si->master_fields_ptr = lpNext(si->lp,si->master_fields_ptr);
    } else {
        *fieldptr = lpGet(si->lp_ele,fieldlen,si->field_buf);
        si->lp_ele = lpNext(si->lp,si->lp_ele);
    }
    *valueptr = lpGet(si->lp_ele,valuelen,si->value_buf);
    si->lp_ele = lpNext(si->lp,si->lp_ele);
}

/* 从 stream 中移除当前条目：可以在 GetID() API 之后或任何 GetField() 调用之后调用，
 * 但调用此函数时我们需要迭代到一个有效条目。此外，该函数需要我们当前迭代的
 * 条目 ID（之前由 GetID() 返回）。
 *
 * 注意，调用此函数后，不能再调用 GetField()：该条目已被删除。
 * 迭代器会自动重新定位到下一个条目，因此调用者应继续使用 GetID()。*/
void streamIteratorRemoveEntry(streamIterator *si, streamID *current) {
    unsigned char *lp = si->lp;
    int64_t aux;

    /* 我们实际上并未真正删除条目。我们只是通过标记它为已删除，
     * 并增加 listpack 头中已删除条目计数来实现。
     *
     * 开始标记：*/
    int64_t flags = lpGetInteger(si->lp_flags);
    flags |= STREAM_ITEM_FLAG_DELETED;
    lp = lpReplaceInteger(lp,&si->lp_flags,flags);

    /* 更改 master 条目中的有效/已删除条目计数。*/
    unsigned char *p = lpFirst(lp);
    aux = lpGetInteger(p);

    if (aux == 1) {
        /* 如果这是 listpack 中的最后一个元素，则可以删除整个节点。*/
        lpFree(lp);
        raxRemove(si->stream->rax,si->ri.key,si->ri.key_len,NULL);
    } else {
        /* 在基础情况下，我们修改有效/已删除条目的计数。*/
        lp = lpReplaceInteger(lp,&p,aux-1);
        p = lpNext(lp,p); /* 定位到 deleted 字段。*/
        aux = lpGetInteger(p);
        lp = lpReplaceInteger(lp,&p,aux+1);

        /* 用新的 listpack 指针更新树。*/
        if (si->lp != lp)
            raxInsert(si->stream->rax,si->ri.key,si->ri.key_len,lp,NULL);
    }

    /* 更新 stream 中的条目计数器。*/
    si->stream->length--;

    /* 重新定位迭代器以修复当前混乱的状态。*/
    streamID start, end;
    if (si->rev) {
        streamDecodeID(si->start_key,&start);
        end = *current;
    } else {
        start = *current;
        streamDecodeID(si->end_key,&end);
    }
    streamIteratorStop(si);
    streamIteratorStart(si,si->stream,&start,&end,si->rev);

    /* TODO: 如果已删除与有效条目数之比超过某个限制，
     * 应在此执行垃圾回收。*/
}

/* 停止 stream 迭代器。唯一需要做的清理工作是释放 rax 迭代器，
 * 因为 stream 迭代器本身应在栈上分配。*/
void streamIteratorStop(streamIterator *si) {
    raxStop(&si->ri);
}

/* 如果 `id` 存在于 `s` 中（且未被标记为已删除），则返回 1 */
int streamEntryExists(stream *s, streamID *id) {
    streamIterator si;
    streamIteratorStart(&si,s,id,id,0);
    streamID myid;
    int64_t numfields;
    int found = streamIteratorGetID(&si,&myid,&numfields);
    streamIteratorStop(&si);
    if (!found)
        return 0;
    serverAssert(streamCompareID(id,&myid) == 0);
    return 1;
}

/* 从 stream 中删除指定的条目 ID。如果条目被删除则返回 1，
 * 否则（如不存在）返回 0。*/
int streamDeleteItem(stream *s, streamID *id) {
    int deleted = 0;
    streamIterator si;
    streamIteratorStart(&si,s,id,id,0);
    streamID myid;
    int64_t numfields;
    if (streamIteratorGetID(&si,&myid,&numfields)) {
        streamIteratorRemoveEntry(&si,&myid);
        deleted = 1;
    }
    streamIteratorStop(&si);
    return deleted;
}

/* 获取 's' 的最后一个有效（非墓碑）streamID。*/
void streamLastValidID(stream *s, streamID *maxid)
{
    streamIterator si;
    streamIteratorStart(&si,s,NULL,NULL,1);
    int64_t numfields;
    if (!streamIteratorGetID(&si,maxid,&numfields) && s->length)
        serverPanic("Corrupt stream, length is %llu, but no max id", (unsigned long long)s->length);
    streamIteratorStop(&si);
}

/* stream ID 字符串的最大大小。理论上 20*2+1 就足够了，
 * 但为了避免差一错误和空终止符的可能性，以防它被用作解析缓冲区，
 * 我们使用稍大一些的缓冲区。另一方面，考虑到 sds 头部会添加 4 个字节，
 * 我们希望保持在分配器的 48 字节桶以下。*/
#define STREAM_ID_STR_LEN 44

sds createStreamIDString(streamID *id) {
    /* 优化：预分配一个足够大的缓冲区以避免重新分配。*/
    sds str = sdsnewlen(SDS_NOINIT, STREAM_ID_STR_LEN);
    sdssetlen(str, 0);
    return sdscatfmt(str,"%U-%U", id->ms,id->seq);
}

/* 在客户端输出缓冲区中以标准的 <ms>-<seq> 格式格式化一个 Stream ID，
 * 并使用 REPL 的简单字符串协议发送响应。*/
void addReplyStreamID(client *c, streamID *id) {
    addReplyBulkSds(c,createStreamIDString(id));
}

void setDeferredReplyStreamID(client *c, void *dr, streamID *id) {
    setDeferredReplyBulkSds(c, dr, createStreamIDString(id));
}

/* 与上述函数类似，但仅创建一个对象，通常用于复制场景以构造参数。*/
robj *createObjectFromStreamID(streamID *id) {
    return createObject(OBJ_STRING, createStreamIDString(id));
}

/* 如果 ID 为 0-0，则返回非零值。*/
int streamIDEqZero(streamID *id) {
    return !(id->ms || id->seq);
}

/* 一个辅助函数：如果从 'start' 到 'end' 的范围内包含墓碑条目，则返回非零值。
 *
 * 注意：此函数假定调用者已经验证 'start' 小于 's->last_id'。*/
int streamRangeHasTombstones(stream *s, streamID *start, streamID *end) {
    streamID start_id, end_id;

    if (!s->length || streamIDEqZero(&s->max_deleted_entry_id)) {
        /* stream 为空或没有墓碑条目。*/
        return 0;
    }

    if (start) {
        start_id = *start;
    } else {
        start_id.ms = 0;
        start_id.seq = 0;
    }

    if (end) {
        end_id = *end;
    } else {
        end_id.ms = UINT64_MAX;
        end_id.seq = UINT64_MAX;
    }

    if (streamCompareID(&start_id,&s->max_deleted_entry_id) <= 0 &&
        streamCompareID(&s->max_deleted_entry_id,&end_id) <= 0)
    {
        /* start_id <= max_deleted_entry_id <= end_id：该范围确实包含一个墓碑条目。*/
        return 1;
    }

    /* 该范围不包含墓碑条目。*/
    return 0;
}

/* 回复消费组的当前 lag，即 stream 中尚未投递的消息数。
 * 如果由于碎片化导致 lag 不可用，则向客户端回复一个 null。*/
void streamReplyWithCGLag(client *c, stream *s, streamCG *cg) {
    int valid = 0;
    long long lag = 0;

    if (!s->entries_added) {
        /* 新初始化的 stream 的 lag 为 0。*/
        lag = 0;
        valid = 1;
    } else if (!s->length) { /* 所有条目已删除，stream 现在为空。*/
        lag = 0;
        valid = 1;
    } else if (streamCompareID(&cg->last_id,&s->first_id) < 0 &&
               streamCompareID(&s->max_deleted_entry_id,&s->first_id) < 0)
    {
        /* 当消费组的 last_id 和最大墓碑条目都在 stream 第一个条目之前时，
         * 消费组的 lag 始终等于 stream 中剩余条目的数量。*/
        lag = s->length;
        valid = 1;
    } else if (cg->entries_read != SCG_INVALID_ENTRIES_READ && !streamRangeHasTombstones(s,&cg->last_id,NULL)) {
        /* 前方没有碎片化意味着该组的逻辑读取计数器可用于 lag 计算。*/
        lag = (long long)s->entries_added - cg->entries_read;
        valid = 1;
    } else {
        /* 尝试获取该组 last ID 的逻辑读取计数器。*/
        long long entries_read = streamEstimateDistanceFromFirstEverEntry(s,&cg->last_id);
        if (entries_read != SCG_INVALID_ENTRIES_READ) {
            /* 已获得有效的计数器。*/
            lag = (long long)s->entries_added - entries_read;
            valid = 1;
        }
    }

    if (valid) {
        addReplyLongLong(c,lag);
    } else {
        addReplyNull(c);
    }
}

/* 此函数返回一个值，它是 ID 的逻辑读取计数器，或其与 stream 中曾经添加的第一个
 * 条目之间的距离（即条目数）。
 *
 * 仅在以下情况之一会返回计数器：
 * 1. ID 与 stream 的最后一个 ID 相同。在这种情况下，返回值与 stream 的
 *    entries_added 计数器相同。
 * 2. ID 等于 stream 当前第一个条目的 ID，且 stream 没有墓碑条目。
 *    这种情况下，返回值是 stream 的 length 与 added_entries 之差再加 1。
 * 3. ID 小于 stream 当前第一个条目的 ID，且没有墓碑条目。
 *    这里的估计计数器是 stream 的 length 与 added_entries 之差。
 * 4. stream 的 added_entries 为 0，意味着从未添加过任何条目。
 *
 * 特殊返回值 ULLONG_MAX 表示无法获得计数器值。在以下情况下会返回该值：
 * 1. 所提供的 ID（即使存在）位于 stream 当前第一个条目 ID 和最后一个条目 ID
 *    之间的某处，或者位于未来。
 * 2. stream 包含一个或多个墓碑条目。*/
long long streamEstimateDistanceFromFirstEverEntry(stream *s, streamID *id) {
    /* 空 stream 中（即从未使用过的 stream），任何 ID 的计数器均为 0。*/
    if (!s->entries_added) {
        return 0;
    }

    /* 在空 stream 中，如果 ID 小于或等于最后一个 ID，
     * 则可以将其设置为当前的 added_entries 值。*/
    if (!s->length && streamCompareID(id,&s->last_id) < 1) {
        return s->entries_added;
    }

    /* 在 `id` 与 stream 最后生成的 ID 之间存在碎片。*/
    if (!streamIDEqZero(id) && streamCompareID(id,&s->max_deleted_entry_id) < 0)
        return SCG_INVALID_ENTRIES_READ;

    int cmp_last = streamCompareID(id,&s->last_id);
    if (cmp_last == 0) {
        /* 返回 stream 中最后一个条目的精确计数器。*/
        return s->entries_added;
    } else if (cmp_last > 0) {
        /* 未来 ID 的计数器未知。*/
        return SCG_INVALID_ENTRIES_READ;
    }

    int cmp_id_first = streamCompareID(id,&s->first_id);
    int cmp_xdel_first = streamCompareID(&s->max_deleted_entry_id,&s->first_id);
    if (streamIDEqZero(&s->max_deleted_entry_id) || cmp_xdel_first < 0) {
        /* 前方肯定没有碎片。*/
        if (cmp_id_first < 0) {
            /* 返回估计的计数器。*/
            return s->entries_added - s->length;
        } else if (cmp_id_first == 0) {
            /* 返回 stream 中第一个条目的精确计数器。*/
            return s->entries_added - s->length + 1;
        }
    }

    /* ID 可能位于导致 stream 碎片化的 XDEL 之前，或者是一个任意 ID。
     * 在这两种情况下，我们都无法进行预测。*/
    return SCG_INVALID_ENTRIES_READ;
}

/* 作为显式 XCLAIM 或 XREADGROUP 命令的结果，会在 stream 和消费者的
 * 待处理列表中创建新条目。我们需要以 XCLAIM 命令的形式传播这些更改。*/
void streamPropagateXCLAIM(client *c, robj *key, streamCG *group, robj *groupname, robj *id, streamNACK *nack) {
    /* 我们需要生成一个以幂等方式工作的 XCLAIM：
     *
     * XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
     *        RETRYCOUNT <count> FORCE JUSTID LASTID <id>.
     *
     * 注意，JUSTID 用于避免 XCLAIM 在从节点上做无用的工作，
     * 即尝试获取 stream 条目。*/
    robj *argv[14];
    argv[0] = shared.xclaim;
    argv[1] = key;
    argv[2] = groupname;
    argv[3] = createStringObject(nack->consumer->name,sdslen(nack->consumer->name));
    argv[4] = shared.integers[0];
    argv[5] = id;
    argv[6] = shared.time;
    argv[7] = createStringObjectFromLongLong(nack->delivery_time);
    argv[8] = shared.retrycount;
    argv[9] = createStringObjectFromLongLong(nack->delivery_count);
    argv[10] = shared.force;
    argv[11] = shared.justid;
    argv[12] = shared.lastid;
    argv[13] = createObjectFromStreamID(&group->last_id);

    alsoPropagate(c->db->id,argv,14,PROPAGATE_AOF|PROPAGATE_REPL);

    decrRefCount(argv[3]);
    decrRefCount(argv[7]);
    decrRefCount(argv[9]);
    decrRefCount(argv[13]);
}

/* 当我们需要传播消费组（被带有 NOACK 选项的 XREADGROUP 消费）的新 last-id 时
 * 需要此函数：在这种情况下，我们不能仅使用 XCLAIM LASTID 选项来传播 last ID，
 * 因此我们发出
 *
 *  XGROUP SETID <key> <groupname> <id> ENTRIESREAD <entries_read>
 */
void streamPropagateGroupID(client *c, robj *key, streamCG *group, robj *groupname) {
    robj *argv[7];
    argv[0] = shared.xgroup;
    argv[1] = shared.setid;
    argv[2] = key;
    argv[3] = groupname;
    argv[4] = createObjectFromStreamID(&group->last_id);
    argv[5] = shared.entriesread;
    argv[6] = createStringObjectFromLongLong(group->entries_read);

    alsoPropagate(c->db->id,argv,7,PROPAGATE_AOF|PROPAGATE_REPL);

    decrRefCount(argv[4]);
    decrRefCount(argv[6]);
}

/* 当我们需要传播由带 NOACK 选项的 XREADGROUP 创建的消费者时需要此函数。
 * 在这种情况下，在副本上创建消费者的唯一方法是使用 XGROUP CREATECONSUMER
 * （参见 issue #7140）
 *
 *  XGROUP CREATECONSUMER <key> <groupname> <consumername>
 */
void streamPropagateConsumerCreation(client *c, robj *key, robj *groupname, sds consumername) {
    robj *argv[5];
    argv[0] = shared.xgroup;
    argv[1] = shared.createconsumer;
    argv[2] = key;
    argv[3] = groupname;
    argv[4] = createObject(OBJ_STRING,sdsdup(consumername));

    alsoPropagate(c->db->id,argv,5,PROPAGATE_AOF|PROPAGATE_REPL);

    decrRefCount(argv[4]);
}

/* 将指定范围内的 stream 条目发送给客户端 'c'。客户端接收的范围是从 start 到 end（含），
 * 如果 'count' 非零，则最多发送 'count' 个元素。
 *
 * 'end' 指针可以为 NULL，表示我们希望从 'start' 到 stream 末尾的所有元素。
 * 如果 'rev' 非零，则按相反顺序（从 end 到 start）输出元素。
 *
 * 该函数返回已输出条目的数量。
 *
 * 如果 group 和 consumer 不为 NULL，该函数会执行额外的工作：
 * 1. 当我们发送的 ID 大于当前的 last ID 时，更新组中的最后投递 ID。
 * 2. 如果请求的 ID 已经分配给其他消费者，该函数不会将其返回给客户端。
 * 3. 对于第一次投递给该消费者的每个条目，会在待处理列表中创建一条记录。
 * 4. 如果组的读取计数器已有效且前方没有墓碑条目，则递增它；否则使其无效（设为 0）。
 *    如果计数器一开始就无效，我们会尝试为最后投递的 ID 获取它。
 *
 * 通过传递非零标志可以修改行为：
 *
 * STREAM_RWR_NOACK：不创建 PEL 条目，即不执行上述第 "3" 点。
 * STREAM_RWR_RAWENTRIES：不输出数组边界的协议，只输出条目本身，
 *                       并照常返回已输出条目的数量。
 *                       当该函数仅用于输出数据且存在更高级别的逻辑时使用。
 *
 * 最后一个参数 'spi'（stream propagation info pointer）是一个结构体，
 * 其中填充了将命令执行传播到 AOF 和从节点所需的信息（在传递了消费组的情况下）：
 * 在这种情况下我们需要生成 XCLAIM 命令来在 AOF/从节点中创建待处理列表。
 *
 * 如果 'spi' 设置为 NULL，即使传递了组，也不会发生任何传播，
 * 但当前代码库从不使用此特性，总是传递 'spi' 并在传递组时进行传播。
 *
 * 注意，在某些情况下此函数是递归的。当使用非 NULL 的 group 和 consumer
 * 参数调用时，它可能会调用 streamReplyWithRangeFromConsumerPEL()
 * 以从消费者的待处理列表中获取条目。但是该函数随后会调用 streamReplyWithRange()
 * 以将单个条目（通过 ID 在 PEL 中找到）输出给客户端。这就是
 * STREAM_RWR_RAWENTRIES 标志的用例。
 */
#define STREAM_RWR_NOACK (1<<0)         /* 不在 PEL 中创建条目。*/
#define STREAM_RWR_RAWENTRIES (1<<1)    /* 不输出数组边界的协议，
                                           仅输出条目。*/
#define STREAM_RWR_HISTORY (1<<2)       /* 仅服务消费者的本地 PEL。*/
size_t streamReplyWithRange(client *c, stream *s, streamID *start, streamID *end, size_t count, int rev, streamCG *group, streamConsumer *consumer, int flags, streamPropInfo *spi, unsigned long *propCount) {
    void *arraylen_ptr = NULL;
    size_t arraylen = 0;
    streamIterator si;
    int64_t numfields;
    streamID id;
    int propagate_last_id = 0;
    int noack = flags & STREAM_RWR_NOACK;

    if (propCount) *propCount = 0;

    /* 如果客户端请求某些历史记录，我们使用一个不同的函数来服务，
     * 这样我们只返回其自身 PEL 中的条目。这确保每个消费者始终且仅能看到
     * 已投递给它但尚未确认的消息的历史记录。*/
    if (group && (flags & STREAM_RWR_HISTORY)) {
        return streamReplyWithRangeFromConsumerPEL(c,s,start,end,count,
                                                   consumer);
    }

    if (!(flags & STREAM_RWR_RAWENTRIES))
        arraylen_ptr = addReplyDeferredLen(c);
    streamIteratorStart(&si,s,start,end,rev);
    while(streamIteratorGetID(&si,&id,&numfields)) {
        /* 如有必要，更新组的 last_id。*/
        if (group && streamCompareID(&id,&group->last_id) > 0) {
            if (group->entries_read != SCG_INVALID_ENTRIES_READ &&
                streamCompareID(&group->last_id, &s->first_id) >= 0 &&
                !streamRangeHasTombstones(s,&group->last_id,NULL))
            {
                /* 在组的 last-delivered-id 和 stream 的 last-generated-id 之间
                 * 具有有效的计数器且没有墓碑条目，意味着我们可以递增读取计数器，
                 * 以继续跟踪组的进度。*/
                group->entries_read++;
            } else if (s->entries_added) {
                /* 组的计数器可能无效，因此我们尝试获取它。*/
                group->entries_read = streamEstimateDistanceFromFirstEverEntry(s,&id);
            }
            group->last_id = id;
            /* 过去，我们仅在指定 NOACK 时才会设置它。在 #9127 中，
             * XCLAIM 在 ACK 时未传播 entries_read，这会导致 master 和副本之间
             * 的 entries_read 不一致，因此这里我们无条件调用 streamPropagateGroupID。*/
            propagate_last_id = 1;
        }

        /* 为每个条目输出一个二元数组。第一个是 ID，第二个是字段值对数组。*/
        addReplyArrayLen(c,2);
        addReplyStreamID(c,&id);

        addReplyArrayLen(c,numfields*2);

        /* 输出字段值对。*/
        while(numfields--) {
            unsigned char *key, *value;
            int64_t key_len, value_len;
            streamIteratorGetField(&si,&key,&value,&key_len,&value_len);
            addReplyBulkCBuffer(c,key,key_len);
            addReplyBulkCBuffer(c,value,value_len);
        }

        /* 如果传递了 group，我们需要在该组 *以及* 该消费者的 PEL
         * （待处理条目列表）中创建一个条目。
         *
         * 注意，我们无法确定该消息是否已被另一个消费者拥有，
         * 因为管理员能够使用 XGROUP SETID 命令更改消费者组的最后投递 ID。
         * 因此，如果我们发现该条目已经存在 NACK，
         * 我们需要将其关联到新的消费者。*/
        if (group && !noack) {
            unsigned char buf[sizeof(streamID)];
            streamEncodeID(buf,&id);

            /* 尝试添加一个新的 NACK。大多数时候这将成功，无需额外的查找。
             * 如果发现该 ID 已经存在条目，我们将稍后修复该问题。*/
            streamNACK *nack = streamCreateNACK(consumer);
            int group_inserted =
                raxTryInsert(group->pel,buf,sizeof(buf),nack,NULL);
            int consumer_inserted =
                raxTryInsert(consumer->pel,buf,sizeof(buf),nack,NULL);

            /* 现在我们可以检查该条目是否已被占用，
             * 如果是，则将该条目重新分配给新的消费者，
             * 或者如果消费者与之前相同则进行更新。*/
            if (group_inserted == 0) {
                streamFreeNACK(nack);
                void *result;
                int found = raxFind(group->pel,buf,sizeof(buf),&result);
                serverAssert(found);
                nack = result;
                raxRemove(nack->consumer->pel,buf,sizeof(buf),NULL);
                /* 更新消费者和 NACK 元数据。*/
                nack->consumer = consumer;
                nack->delivery_time = commandTimeSnapshot();
                nack->delivery_count = 1;
                /* 在新消费者的本地 PEL 中添加该条目。*/
                raxInsert(consumer->pel,buf,sizeof(buf),nack,NULL);
            } else if (group_inserted == 1 && consumer_inserted == 0) {
                serverPanic("NACK half-created. Should not be possible.");
            }

            consumer->active_time = commandTimeSnapshot();

            /* 以 XCLAIM 的形式进行传播。*/
            if (spi) {
                robj *idarg = createObjectFromStreamID(&id);
                streamPropagateXCLAIM(c,spi->keyname,group,spi->groupname,idarg,nack);
                decrRefCount(idarg);
                if (propCount) (*propCount)++;
            }
        }

        arraylen++;
        if (count && count == arraylen) break;
    }

    if (spi && propagate_last_id) {
        streamPropagateGroupID(c,spi->keyname,group,spi->groupname);
        if (propCount) (*propCount)++;
    }

    streamIteratorStop(&si);
    if (arraylen_ptr) setDeferredArrayLen(c,arraylen_ptr,arraylen);
    return arraylen;
}

/* 这是 streamReplyWithRange() 在使用 group 和 consumer 参数调用，但范围引用的是
 * 已经投递的消息时的辅助函数。在这种情况下，我们只需输出已经在消费者历史记录中
 * 的消息，从其 PEL 中获取 ID。
 *
 * 注意，此函数没有 'rev' 参数，因为在使用组时无法反向迭代。
 * 基本上，此函数仅在 XREADGROUP 命令的结果中被调用。
 *
 * 该函数开销较大，因为它需要检查 PEL，然后在消息的基数树中定位以将完整消息
 * 输出给客户端。但是，客户端仅在获取已检索消息的历史记录时才会进入此代码路径，
 * 这种情况很少见。*/
size_t streamReplyWithRangeFromConsumerPEL(client *c, stream *s, streamID *start, streamID *end, size_t count, streamConsumer *consumer) {
    raxIterator ri;
    unsigned char startkey[sizeof(streamID)];
    unsigned char endkey[sizeof(streamID)];
    streamEncodeID(startkey,start);
    if (end) streamEncodeID(endkey,end);

    size_t arraylen = 0;
    void *arraylen_ptr = addReplyDeferredLen(c);
    raxStart(&ri,consumer->pel);
    raxSeek(&ri,">=",startkey,sizeof(startkey));
    while(raxNext(&ri) && (!count || arraylen < count)) {
        if (end && memcmp(ri.key,end,ri.key_len) > 0) break;
        streamID thisid;
        streamDecodeID(ri.key,&thisid);
        if (streamReplyWithRange(c,s,&thisid,&thisid,1,0,NULL,NULL,
                                 STREAM_RWR_RAWENTRIES,NULL,NULL) == 0)
        {
            /* 注意，PEL 中可能存在一个未确认的条目，其对应的消息已不再存在，
             * 因为用户已通过其他方式删除了它。在这种情况下，
             * 我们通过输出该 ID 但为字段输出 NULL 来表示它。*/
            addReplyArrayLen(c,2);
            addReplyStreamID(c,&thisid);
            addReplyNullArray(c);
        } else {
            streamNACK *nack = ri.data;
            nack->delivery_time = commandTimeSnapshot();
            nack->delivery_count++;
        }
        arraylen++;
    }
    raxStop(&ri);
    setDeferredArrayLen(c,arraylen_ptr,arraylen);
    return arraylen;
}

/* -----------------------------------------------------------------------
 * Stream 命令实现
 * ----------------------------------------------------------------------- */

/* 在 'key' 处查找 stream 并返回对应的 stream 对象。
 * 如有必要，函数会创建一个设置为空 stream 的 key。*/
robj *streamTypeLookupWriteOrCreate(client *c, robj *key, int no_create) {
    robj *o = lookupKeyWrite(c->db,key);
    if (checkType(c,o,OBJ_STREAM)) return NULL;
    if (o == NULL) {
        if (no_create) {
            addReplyNull(c);
            return NULL;
        }
        o = createStreamObject();
        dbAdd(c->db,key,o);
    }
    return o;
}

/* 解析客户端提供给 Redis 的格式（即 <ms>-<seq>）的 stream ID，
 * 并将其转换为 streamID 结构。如果指定的 ID 无效，则返回 C_ERR
 * 并向客户端报告错误；否则返回 C_OK。
 * 该 ID 可能是不完整的，仅声明了 stream 的毫秒时间部分。在这种情况下，
 * 缺失的部分根据 'missing_seq' 参数的值进行设置。
 *
 * ID "-" 和 "+" 分别指定可表示的最小和最大 ID。如果 'strict' 设置为 1，
 * "-" 和 "+" 将被视为无效 ID。
 *
 * ID 形式 <ms>-* 指定仅包含毫秒的 ID，序列号部分将自动生成。
 * 当提供非 NULL 的 'seq_given' 参数时，接受这种形式，
 * 并且除非指定了序列号部分，否则该参数将被设置为 0。
 *
 * 如果 'c' 设置为 NULL，则不会向客户端发送回复。*/
int streamGenericParseIDOrReply(client *c, const robj *o, streamID *id, uint64_t missing_seq, int strict, int *seq_given) {
    char buf[128];
    if (sdslen(o->ptr) > sizeof(buf)-1) goto invalid;
    memcpy(buf,o->ptr,sdslen(o->ptr)+1);

    if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0')
        goto invalid;

    if (seq_given != NULL) {
        *seq_given = 1;
    }

    /* 处理 "-" 和 "+" 特殊情况。*/
    if (buf[0] == '-' && buf[1] == '\0') {
        id->ms = 0;
        id->seq = 0;
        return C_OK;
    } else if (buf[0] == '+' && buf[1] == '\0') {
        id->ms = UINT64_MAX;
        id->seq = UINT64_MAX;
        return C_OK;
    }

    /* 解析 <ms>-<seq> 形式。*/
    unsigned long long ms, seq;
    char *dot = strchr(buf,'-');
    if (dot) *dot = '\0';
    if (string2ull(buf,&ms) == 0) goto invalid;
    if (dot) {
        size_t seqlen = strlen(dot+1);
        if (seq_given != NULL && seqlen == 1 && *(dot + 1) == '*') {
            /* 处理 <ms>-* 形式。*/
            seq = 0;
            *seq_given = 0;
        } else if (string2ull(dot+1,&seq) == 0) {
            goto invalid;
        }
    } else {
        seq = missing_seq;
    }
    id->ms = ms;
    id->seq = seq;
    return C_OK;

invalid:
    if (c) addReplyError(c,"作为 stream 命令参数的 stream ID 无效");
    return C_ERR;
}

/* streamGenericParseIDOrReply() 的包装函数，供模块 API 使用。*/
int streamParseID(const robj *o, streamID *id) {
    return streamGenericParseIDOrReply(NULL,o,id,0,0,NULL);
}

/* streamGenericParseIDOrReply() 的包装函数，'strict' 参数设置为 0，
 * 用于 - 和 + 是可接受的 ID 时。*/
int streamParseIDOrReply(client *c, robj *o, streamID *id, uint64_t missing_seq) {
    return streamGenericParseIDOrReply(c,o,id,missing_seq,0,NULL);
}

/* streamGenericParseIDOrReply() 的包装函数，'strict' 参数设置为 1，
 * 用于在提供特殊 ID + 或 - 时希望返回错误的情况。*/
int streamParseStrictIDOrReply(client *c, robj *o, streamID *id, uint64_t missing_seq, int *seq_given) {
    return streamGenericParseIDOrReply(c,o,id,missing_seq,1,seq_given);
}

/* 用于解析作为范围查询区间的 stream ID 的辅助函数。
 * 当 exclude 参数为 NULL 时，调用 streamParseIDOrReply()，
 * 该区间被视为闭区间（含端点）。否则，如果该区间是开区间
 * （带有 "(" 前缀），则设置 exclude 参数，并调用 streamParseStrictIDOrReply()。
 */
int streamParseIntervalIDOrReply(client *c, robj *o, streamID *id, int *exclude, uint64_t missing_seq) {
    char *p = o->ptr;
    size_t len = sdslen(p);
    int invalid = 0;
    
    if (exclude != NULL) *exclude = (len > 1 && p[0] == '(');
    if (exclude != NULL && *exclude) {
        robj *t = createStringObject(p+1,len-1);
        invalid = (streamParseStrictIDOrReply(c,t,id,missing_seq,NULL) == C_ERR);
        decrRefCount(t);
    } else 
        invalid = (streamParseIDOrReply(c,o,id,missing_seq) == C_ERR);
    if (invalid)
        return C_ERR;
    return C_OK;
}

void streamRewriteApproxSpecifier(client *c, int idx) {
    rewriteClientCommandArgument(c,idx,shared.special_equals);
}

/* 我们将 MAXLEN/MINID ~ <count> 传播为 MAXLEN/MINID = <resulting-len-of-stream>，
 * 否则修剪在副本/AOF 上不再是确定性的。*/
void streamRewriteTrimArgument(client *c, stream *s, int trim_strategy, int idx) {
    robj *arg;
    if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
        arg = createStringObjectFromLongLong(s->length);
    } else {
        streamID first_id;
        streamGetEdgeID(s,1,0,&first_id);
        arg = createObjectFromStreamID(&first_id);
    }

    rewriteClientCommandArgument(c,idx,arg);
    decrRefCount(arg);
}

/* XADD key [(MAXLEN [~|=] <count> | MINID [~|=] <id>) [LIMIT <entries>]] [NOMKSTREAM] <ID or *> [field value] [field value] ... */
void xaddCommand(client *c) {
    /* 解析选项。*/
    streamAddTrimArgs parsed_args;
    int idpos = streamParseAddOrTrimArgsOrReply(c, &parsed_args, 1);
    if (idpos < 0)
        return; /* streamParseAddOrTrimArgsOrReply 已发送回复。*/
    int field_pos = idpos+1; /* ID 始终是第一个字段之前的一个参数 */

    /* 检查参数数量。*/
    if ((c->argc - field_pos) < 2 || ((c->argc-field_pos) % 2) == 1) {
        addReplyErrorArity(c);
        return;
    }

    /* 如果给出了最小 ID (0-0)，则立即返回，以避免可能创建新 stream
     * 并导致 streamAppendItem 失败而在数据库中留下一个空 key。*/
    if (parsed_args.id_given && parsed_args.seq_given &&
        parsed_args.id.ms == 0 && parsed_args.id.seq == 0)
    {
        addReplyError(c,"XADD 中指定的 ID 必须大于 0-0");
        return;
    }

    /* 在 key 处查找 stream。*/
    robj *o;
    stream *s;
    if ((o = streamTypeLookupWriteOrCreate(c,c->argv[1],parsed_args.no_mkstream)) == NULL) return;
    s = o->ptr;

    /* 如果 stream 已达到最后一个可能的 ID，则立即返回 */
    if (s->last_id.ms == UINT64_MAX && s->last_id.seq == UINT64_MAX) {
        addReplyError(c,"stream 已用尽最后一个可能的 ID，"
                        "无法再添加更多条目");
        return;
    }

    /* 使用底层函数追加并返回 ID。*/
    errno = 0;
    streamID id;
    if (streamAppendItem(s,c->argv+field_pos,(c->argc-field_pos)/2,
        &id,parsed_args.id_given ? &parsed_args.id : NULL,parsed_args.seq_given) == C_ERR)
    {
        serverAssert(errno != 0);
        if (errno == EDOM)
            addReplyError(c,"XADD 中指定的 ID 等于或小于 "
                            "目标 stream 的顶部条目");
        else
            addReplyError(c,"元素过大而无法存储");
        return;
    }
    sds replyid = createStreamIDString(&id);
    addReplyBulkCBuffer(c, replyid, sdslen(replyid));

    notifyKeyspaceEvent(NOTIFY_STREAM,"xadd",c->argv[1],c->db->id);
    server.dirty++;

    /* 如果需要则执行 trim。*/
    if (parsed_args.trim_strategy != TRIM_STRATEGY_NONE) {
        if (streamTrim(s, &parsed_args)) {
            notifyKeyspaceEvent(NOTIFY_STREAM,"xtrim",c->argv[1],c->db->id);
        }
        if (parsed_args.approx_trim) {
            /* 如果我们的修剪被限制（通过 LIMIT 或 ~），我们必须
             * 重写相关的 trim 参数，以确保在 AOF 加载或副本中不会出现不一致。
             * 只需检查 args->approx 就足够了，因为不存在 LIMIT 在没有 ~ 选项
             * 的情况下被给出的情况。*/
            streamRewriteApproxSpecifier(c,parsed_args.trim_strategy_arg_idx-1);
            streamRewriteTrimArgument(c,s,parsed_args.trim_strategy,parsed_args.trim_strategy_arg_idx);
        }
    }

    signalModifiedKey(c,c->db,c->argv[1]);

    /* 让我们用实际生成的 ID 重写 ID 参数，以用于 AOF/复制传播。*/
    if (!parsed_args.id_given || !parsed_args.seq_given) {
        robj *idarg = createObject(OBJ_STRING, replyid);
        rewriteClientCommandArgument(c, idpos, idarg);
        decrRefCount(idarg);
    } else {
        sdsfree(replyid);
    }

    /* 我们需要向被阻塞的客户端发出信号，告知此 stream 上有新数据。*/
    signalKeyAsReady(c->db, c->argv[1], OBJ_STREAM);
}

/* XRANGE/XREVRANGE 的实际实现。
 * 'start' 和 'end' ID 按以下方式解析：
 *   不完整的 'start' 的序列号设置为 0，'end' 设置为 UINT64_MAX。
 *   "-" 和 "+" 分别表示最小和最大 ID 值。
 *   "(" 前缀表示开（排他）区间，所以 XRANGE stream (1-0 (2-0
 *   将匹配从 1-1 到 1-UINT64_MAX 之间的任何内容。
 */
void xrangeGenericCommand(client *c, int rev) {
    robj *o;
    stream *s;
    streamID startid, endid;
    long long count = -1;
    robj *startarg = rev ? c->argv[3] : c->argv[2];
    robj *endarg = rev ? c->argv[2] : c->argv[3];
    int startex = 0, endex = 0;

    /* 解析 start 和 end ID。*/
    if (streamParseIntervalIDOrReply(c,startarg,&startid,&startex,0) != C_OK)
        return;
    if (startex && streamIncrID(&startid) != C_OK) {
        addReplyError(c,"区间的无效 start ID");
        return;
    }
    if (streamParseIntervalIDOrReply(c,endarg,&endid,&endex,UINT64_MAX) != C_OK)
        return;
    if (endex && streamDecrID(&endid) != C_OK) {
        addReplyError(c,"区间的无效 end ID");
        return;
    }

    /* 如果存在 COUNT 选项则进行解析。*/
    if (c->argc > 4) {
        for (int j = 4; j < c->argc; j++) {
            int additional = c->argc-j-1;
            if (strcasecmp(c->argv[j]->ptr,"COUNT") == 0 && additional >= 1) {
                if (getLongLongFromObjectOrReply(c,c->argv[j+1],&count,NULL)
                    != C_OK) return;
                if (count < 0) count = 0;
                j++; /* 消耗额外参数。*/
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* 将指定的范围返回给用户。*/
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray)) == NULL ||
         checkType(c,o,OBJ_STREAM)) return;

    s = o->ptr;

    if (count == 0) {
        addReplyNullArray(c);
    } else {
        if (count == -1) count = 0;
        streamReplyWithRange(c,s,&startid,&endid,count,rev,NULL,NULL,0,NULL,NULL);
    }
}

/* XRANGE key start end [COUNT <n>] */
void xrangeCommand(client *c) {
    xrangeGenericCommand(c,0);
}

/* XREVRANGE key end start [COUNT <n>] */
void xrevrangeCommand(client *c) {
    xrangeGenericCommand(c,1);
}

/* XLEN key*/
void xlenCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL
        || checkType(c,o,OBJ_STREAM)) return;
    stream *s = o->ptr;
    addReplyLongLong(c,s->length);
}

/* XREAD [BLOCK <milliseconds>] [COUNT <count>] STREAMS key_1 key_2 ... key_N
 *       ID_1 ID_2 ... ID_N
 *
 * 此函数还实现了 XREADGROUP 命令，它类似于 XREAD，但接受额外的
 * [GROUP group-name consumer-name] 选项。这很有用，因为 XREAD 是读命令，
 * 可以在从节点上调用，而 XREADGROUP 不行。*/
#define XREAD_BLOCKED_DEFAULT_COUNT 1000
void xreadCommand(client *c) {
    long long timeout = -1; /* -1 表示未给出 BLOCK 参数。*/
    long long count = 0;
    int streams_count = 0;
    int streams_arg = 0;
    int noack = 0;          /* 如果指定了 NOACK 选项则为 true。*/
    streamID static_ids[STREAMID_STATIC_VECTOR_LEN];
    streamID *ids = static_ids;
    streamCG **groups = NULL;
    int xreadgroup = sdslen(c->argv[0]->ptr) == 10; /* XREAD 还是 XREADGROUP？*/
    robj *groupname = NULL;
    robj *consumername = NULL;

    /* 解析参数。*/
    for (int i = 1; i < c->argc; i++) {
        int moreargs = c->argc-i-1;
        char *o = c->argv[i]->ptr;
        if (!strcasecmp(o,"BLOCK") && moreargs) {
            i++;
            if (getTimeoutFromObjectOrReply(c,c->argv[i],&timeout,
                UNIT_MILLISECONDS) != C_OK) return;
        } else if (!strcasecmp(o,"COUNT") && moreargs) {
            i++;
            if (getLongLongFromObjectOrReply(c,c->argv[i],&count,NULL) != C_OK)
                return;
            if (count < 0) count = 0;
        } else if (!strcasecmp(o,"STREAMS") && moreargs) {
            streams_arg = i+1;
            streams_count = (c->argc-streams_arg);
            if ((streams_count % 2) != 0) {
                char symbol = xreadgroup ? '>' : '$';
                addReplyErrorFormat(c,"'%s' 的 stream 列表不平衡："
                                      "每个 stream key 必须指定一个 ID 或 '%c'。",
                                      c->cmd->fullname,symbol);
                return;
            }
            streams_count /= 2; /* 每个 stream 有两个参数。*/
            break;
        } else if (!strcasecmp(o,"GROUP") && moreargs >= 2) {
            if (!xreadgroup) {
                addReplyError(c,"GROUP 选项仅由 XREADGROUP 支持。"
                                "你调用的是 XREAD。");
                return;
            }
            groupname = c->argv[i+1];
            consumername = c->argv[i+2];
            i += 2;
        } else if (!strcasecmp(o,"NOACK")) {
            if (!xreadgroup) {
                addReplyError(c,"NOACK 选项仅由 XREADGROUP 支持。"
                                "你调用的是 XREAD。");
                return;
            }
            noack = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* STREAMS 选项是必需的。*/
    if (streams_arg == 0) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 如果用户指定了 XREADGROUP，那么还必须提供 GROUP 选项。*/
    if (xreadgroup && groupname == NULL) {
        addReplyError(c,"XREADGROUP 缺少 GROUP 选项");
        return;
    }

    /* 解析 ID 并解析组名。*/
    if (streams_count > STREAMID_STATIC_VECTOR_LEN)
        ids = zmalloc(sizeof(streamID)*streams_count);
    if (groupname) groups = zmalloc(sizeof(streamCG*)*streams_count);

    for (int i = streams_arg + streams_count; i < c->argc; i++) {
        /* 将 "$" 指定为最后已知 ID 意味着客户端希望仅获取从现在起
         * 将到达 stream 中的消息。*/
        int id_idx = i - streams_arg - streams_count;
        robj *key = c->argv[i-streams_count];
        robj *o = lookupKeyRead(c->db,key);
        if (checkType(c,o,OBJ_STREAM)) goto cleanup;
        streamCG *group = NULL;

        /* 如果指定了 group，则我们需要确保 key 和 group 确实存在。*/
        if (groupname) {
            if (o == NULL ||
                (group = streamLookupCG(o->ptr,groupname->ptr)) == NULL)
            {
                addReplyErrorFormat(c, "-NOGROUP 在带 GROUP 选项的 XREADGROUP 中，"
                                       "不存在 key '%s' 或消费者组 '%s'",
                                    (char*)key->ptr,(char*)groupname->ptr);
                goto cleanup;
            }
            groups[id_idx] = group;
        }

        if (strcmp(c->argv[i]->ptr,"$") == 0) {
            if (xreadgroup) {
                addReplyError(c,"在 XREADGROUP 的上下文中 $ ID 没有意义："
                                "你希望通过指定一个适当的 ID 来读取此消费者的历史记录，"
                                "或者使用 > ID 来获取新消息。"
                                "$ ID 只会返回一个空结果集。");
                goto cleanup;
            }
            if (o) {
                stream *s = o->ptr;
                ids[id_idx] = s->last_id;
            } else {
                ids[id_idx].ms = 0;
                ids[id_idx].seq = 0;
            }
            continue;
        } else if (strcmp(c->argv[i]->ptr,"+") == 0) {
            if (xreadgroup) {
                addReplyError(c,"在 XREADGROUP 的上下文中 + ID 没有意义："
                                "你希望通过指定一个适当的 ID 来读取此消费者的历史记录，"
                                "或者使用 > ID 来获取新消息。"
                                "+ ID 只会返回一个空结果集。");
                goto cleanup;
            }
            if (o && ((stream *)o->ptr)->length) {
                stream *s = o->ptr;
                /* 我们需要获取最后一个有效的 ID。
                 * 不能使用 s->last_id，因为 ID 为 s->last_id 的条目可能已被删除。*/
                streamLastValidID(s, &ids[id_idx]);
                streamDecrID(&ids[id_idx]);
            } else {
                ids[id_idx].ms = 0;
                ids[id_idx].seq = 0;
            }
            continue;
        } else if (strcmp(c->argv[i]->ptr,">") == 0) {
            if (!xreadgroup) {
                addReplyError(c,"仅在使用 GROUP <group> <consumer> 选项"
                                "调用 XREADGROUP 时才能指定 > ID。");
                goto cleanup;
            }
            /* 我们仅使用最大 ID 来表示这是 ">" ID，
             * 处理阻塞客户端的代码稍后必须更新 ID，
             * 以匹配不断变化的消费者组 last ID。*/
            ids[id_idx].ms = UINT64_MAX;
            ids[id_idx].seq = UINT64_MAX;
            continue;
        }
        if (streamParseStrictIDOrReply(c,c->argv[i],ids+id_idx,0,NULL) != C_OK)
            goto cleanup;
    }

    /* 尝试同步服务客户端。*/
    size_t arraylen = 0;
    void *arraylen_ptr = NULL;
    for (int i = 0; i < streams_count; i++) {
        robj *o = lookupKeyRead(c->db,c->argv[streams_arg+i]);
        if (o == NULL) continue;
        stream *s = o->ptr;
        streamID *gt = ids+i; /* ID 必须大于此值。*/
        int serve_synchronously = 0;
        int serve_history = 0; /* 对于 ID != ">" 的 XREADGROUP 为 true。*/
        streamConsumer *consumer = NULL; /* XREAD 时不使用 */
        streamPropInfo spi = {c->argv[streams_arg+i],groupname}; /* XREAD 时不使用 */

        /* 检查是否有同步服务客户端的条件。*/
        if (groups) {
            /* 如果消费者阻塞在一个组上，当指定的 ID 不是特殊的 ">" ID 时，
             * 我们始终同步为其提供服务（即服务其本地历史）。*/
            if (gt->ms != UINT64_MAX ||
                gt->seq != UINT64_MAX)
            {
                serve_synchronously = 1;
                serve_history = 1;
            } else if (s->length) {
                /* 当消费者组已投递的顶部条目小于 stream 中实际包含的内容时，
                 * 我们也希望同步服务该消费者组。*/
                streamID maxid, *last = &groups[i]->last_id;
                streamLastValidID(s, &maxid);
                if (streamCompareID(&maxid, last) > 0) {
                    serve_synchronously = 1;
                    *gt = *last;
                }
            }
            consumer = streamLookupConsumer(groups[i],consumername->ptr);
            if (consumer == NULL) {
                consumer = streamCreateConsumer(groups[i],consumername->ptr,
                                                c->argv[streams_arg+i],
                                                c->db->id,SCC_DEFAULT);
                if (noack)
                    streamPropagateConsumerCreation(c,spi.keyname,
                                                    spi.groupname,
                                                    consumer->name);
            }
            consumer->seen_time = commandTimeSnapshot();
        } else if (s->length) {
            /* 对于不在组中的消费者，如果实际上能从 stream 提供至少一个条目，
             * 我们就同步服务。*/
            streamID maxid;
            streamLastValidID(s, &maxid);
            if (streamCompareID(&maxid, gt) > 0) {
                serve_synchronously = 1;
            }
        }

        if (serve_synchronously) {
            arraylen++;
            if (arraylen == 1) arraylen_ptr = addReplyDeferredLen(c);
            /* streamReplyWithRange() 将 'start' ID 视为包含端点，
             * 所以从下一个 ID 开始，因为我们只想要 ID 大于 start 的消息。*/
            streamID start = *gt;
            streamIncrID(&start);

            /* 输出由 stream 名称和我们从中提取的数据组成的二元子数组。*/
            if (c->resp == 2) addReplyArrayLen(c,2);
            addReplyBulk(c,c->argv[streams_arg+i]);

            int flags = 0;
            unsigned long propCount = 0;
            if (noack) flags |= STREAM_RWR_NOACK;
            if (serve_history) flags |= STREAM_RWR_HISTORY;
            streamReplyWithRange(c,s,&start,NULL,count,0,
                                 groups ? groups[i] : NULL,
                                 consumer, flags, &spi, &propCount);
            if (propCount) server.dirty++;
        }
    }

     /* 我们已同步回复！设置顶层数组长度并返回调用方。*/
    if (arraylen) {
        if (c->resp == 2)
            setDeferredArrayLen(c,arraylen_ptr,arraylen);
        else
            setDeferredMapLen(c,arraylen_ptr,arraylen);
        goto cleanup;
    }

    /* 如果需要则进行阻塞。*/
    if (timeout != -1) {
        /* 如果我们不允许阻塞客户端，那么唯一能做的
         * 就是将其视为超时（即使超时为 0）。*/
        if (c->flags & CLIENT_DENY_BLOCKING) {
            addReplyNullArray(c);
            goto cleanup;
        }
        /* 我们将 '$' 更改为该 stream 的当前最后 ID。
         * 因为稍后在有新数据时解除阻塞时——我们希望
         * 重新处理该命令，如果 '$' 保持不变，我们将永远自旋阻塞。
         */
        for (int id_idx = 0; id_idx < streams_count; id_idx++) {
            int arg_idx = id_idx + streams_arg + streams_count;
            if (strcmp(c->argv[arg_idx]->ptr,"$") == 0) {
                robj *argv_streamid = createObjectFromStreamID(&ids[id_idx]);
                rewriteClientCommandArgument(c, arg_idx, argv_streamid);
                decrRefCount(argv_streamid);
            }
        }
        blockForKeys(c, BLOCKED_STREAM, c->argv+streams_arg, streams_count, timeout, xreadgroup);
        goto cleanup;
    }

    /* 没有 BLOCK 选项，且没有可以服务的 stream。
     * 按超时发生的方式回复。*/
    addReplyNullArray(c);
    /* 继续执行 cleanup... */

cleanup: /* 清理。*/

    /* 命令（以 READGROUP 形式）作为调用低层 API 的副作用被传播。
     * 因此停止任何隐式传播。*/
    preventCommandPropagation(c);
    if (ids != static_ids) zfree(ids);
    zfree(groups);
}

/* -----------------------------------------------------------------------
 * 消费组的底层实现
 * ----------------------------------------------------------------------- */

/* 创建一个 NACK 条目，将投递计数设置为 1，投递时间设置为当前时间。
 * NACK 的 consumer 将被设置为函数参数中指定的那个。*/
streamNACK *streamCreateNACK(streamConsumer *consumer) {
    streamNACK *nack = zmalloc(sizeof(*nack));
    nack->delivery_time = commandTimeSnapshot();
    nack->delivery_count = 1;
    nack->consumer = consumer;
    return nack;
}

/* 释放一个 NACK 条目。*/
void streamFreeNACK(streamNACK *na) {
    zfree(na);
}

/* 释放一个消费者及其关联的数据结构。注意，此函数不会重新分配
 * 与该消费者关联的待处理消息，也不会从 stream 中删除它们，
 * 因此当调用此函数删除消费者（而非销毁整个 stream）时，
 * 调用者应在之前做一些工作。*/
void streamFreeConsumer(streamConsumer *sc) {
    raxFree(sc->pel); /* 没有值释放回调：PEL 条目在消费者和主 stream PEL 之间共享。*/
    sdsfree(sc->name);
    zfree(sc);
}

/* 在 stream 's' 的上下文中创建一个具有指定名称、最后服务器 ID 和读取计数器的新消费组。
 * 如果已存在同名的消费组，则返回 NULL，否则返回指向该消费组的指针。*/
streamCG *streamCreateCG(stream *s, char *name, size_t namelen, streamID *id, long long entries_read) {
    if (s->cgroups == NULL) s->cgroups = raxNew();
    if (raxFind(s->cgroups,(unsigned char*)name,namelen,NULL))
        return NULL;

    streamCG *cg = zmalloc(sizeof(*cg));
    cg->pel = raxNew();
    cg->consumers = raxNew();
    cg->last_id = *id;
    cg->entries_read = entries_read;
    raxInsert(s->cgroups,(unsigned char*)name,namelen,cg,NULL);
    return cg;
}

/* 释放一个消费组及其所有关联数据。*/
void streamFreeCG(streamCG *cg) {
    raxFreeWithCallback(cg->pel,(void(*)(void*))streamFreeNACK);
    raxFreeWithCallback(cg->consumers,(void(*)(void*))streamFreeConsumer);
    zfree(cg);
}

/* 在指定的 stream 中查找消费组并返回其指针，如果没有这样的组，则返回 NULL。*/
streamCG *streamLookupCG(stream *s, sds groupname) {
    if (s->cgroups == NULL) return NULL;
    void *cg = NULL;
    raxFind(s->cgroups,(unsigned char*)groupname,sdslen(groupname),&cg);
    return cg;
}

/* 在组 'cg' 中创建一个具有指定名称的消费者并返回。
 * 如果该消费者已存在，则返回 NULL。作为副作用，当消费者成功创建后，
 * 除非指定了 SCC_NO_NOTIFY 或 SCC_NO_DIRTIFY 标志，否则会通知 key 空间并执行 dirty++。*/
streamConsumer *streamCreateConsumer(streamCG *cg, sds name, robj *key, int dbid, int flags) {
    if (cg == NULL) return NULL;
    int notify = !(flags & SCC_NO_NOTIFY);
    int dirty = !(flags & SCC_NO_DIRTIFY);
    streamConsumer *consumer = zmalloc(sizeof(*consumer));
    int success = raxTryInsert(cg->consumers,(unsigned char*)name,
                               sdslen(name),consumer,NULL);
    if (!success) {
        zfree(consumer);
        return NULL;
    }
    consumer->name = sdsdup(name);
    consumer->pel = raxNew();
    consumer->active_time = -1;
    consumer->seen_time = commandTimeSnapshot();
    if (dirty) server.dirty++;
    if (notify) notifyKeyspaceEvent(NOTIFY_STREAM,"xgroup-createconsumer",key,dbid);
    return consumer;
}

/* 在组 'cg' 中查找具有指定名称的消费者。*/
streamConsumer *streamLookupConsumer(streamCG *cg, sds name) {
    if (cg == NULL) return NULL;
    void *consumer = NULL;
    raxFind(cg->consumers,(unsigned char*)name,sdslen(name),&consumer);
    return consumer;
}

/* 删除消费组 'cg' 中指定的消费者。*/
void streamDelConsumer(streamCG *cg, streamConsumer *consumer) {
    /* 遍历该消费者的所有待处理消息，从全局条目中删除每个对应的条目。*/
    raxIterator ri;
    raxStart(&ri,consumer->pel);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        streamNACK *nack = ri.data;
        raxRemove(cg->pel,ri.key,ri.key_len,NULL);
        streamFreeNACK(nack);
    }
    raxStop(&ri);

    /* 释放消费者。*/
    raxRemove(cg->consumers,(unsigned char*)consumer->name,
              sdslen(consumer->name),NULL);
    streamFreeConsumer(consumer);
}

/* -----------------------------------------------------------------------
 * 消费组命令
 * ----------------------------------------------------------------------- */

/* XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]
 * XGROUP SETID <key> <groupname> <id or $> [ENTRIESREAD entries_read]
 * XGROUP DESTROY <key> <groupname>
 * XGROUP CREATECONSUMER <key> <groupname> <consumer>
 * XGROUP DELCONSUMER <key> <groupname> <consumername> */
void xgroupCommand(client *c) {
    stream *s = NULL;
    sds grpname = NULL;
    streamCG *cg = NULL;
    char *opt = c->argv[1]->ptr; /* 子命令名称。*/
    int mkstream = 0;
    long long entries_read = SCG_INVALID_ENTRIES_READ;
    robj *o;

    /* 除 "HELP" 选项外的所有操作都需要 key 和组名。*/
    if (c->argc >= 4) {
        /* 解析 CREATE 和 SETID 的可选参数 */
        int i = 5;
        int create_subcmd = !strcasecmp(opt,"CREATE");
        int setid_subcmd = !strcasecmp(opt,"SETID");
        while (i < c->argc) {
            if (create_subcmd && !strcasecmp(c->argv[i]->ptr,"MKSTREAM")) {
                mkstream = 1;
                i++;
            } else if ((create_subcmd || setid_subcmd) && !strcasecmp(c->argv[i]->ptr,"ENTRIESREAD") && i + 1 < c->argc) {
                if (getLongLongFromObjectOrReply(c,c->argv[i+1],&entries_read,NULL) != C_OK)
                    return;
                if (entries_read < 0 && entries_read != SCG_INVALID_ENTRIES_READ) {
                    addReplyError(c,"ENTRIESREAD 的值必须为正数或 -1");
                    return;
                }
                i += 2;
            } else {
                addReplySubcommandSyntaxError(c);
                return;
            }
        }

        o = lookupKeyWrite(c->db,c->argv[2]);
        if (o) {
            if (checkType(c,o,OBJ_STREAM)) return;
            s = o->ptr;
        }
        grpname = c->argv[3]->ptr;
    }

    /* 检查是否缺少 key/组。*/
    if (c->argc >= 4 && !mkstream) {
        /* 此时 key 必须存在，否则会出错。*/
        if (s == NULL) {
            addReplyError(c,
                "XGROUP 子命令要求 key 必须存在。"
                "注意，对于 CREATE，你可能希望使用 MKSTREAM 选项"
                "来自动创建一个空的 stream。");
            return;
        }

        /* 某些子命令要求组必须存在。*/
        if ((cg = streamLookupCG(s,grpname)) == NULL &&
            (!strcasecmp(opt,"SETID") ||
             !strcasecmp(opt,"CREATECONSUMER") ||
             !strcasecmp(opt,"DELCONSUMER")))
        {
            addReplyErrorFormat(c, "-NOGROUP key '%s' 上不存在消费者组 '%s'",
                                   (char*)grpname, (char*)c->argv[2]->ptr);
            return;
        }
    }

    /* 分发不同的子命令。*/
    if (c->argc == 2 && !strcasecmp(opt,"HELP")) {
        const char *help[] = {
"CREATE <key> <groupname> <id|$> [option]",
"    创建一个新的消费者组。选项包括：",
"    * MKSTREAM",
"      如果 stream 不存在则创建空 stream。",
"    * ENTRIESREAD entries_read",
"      设置组的 entries_read 计数器（内部使用）。",
"CREATECONSUMER <key> <groupname> <consumer>",
"    在指定组中创建一个新的消费者。",
"DELCONSUMER <key> <groupname> <consumer>",
"    删除指定的消费者。",
"DESTROY <key> <groupname>",
"    删除指定的组。",
"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
"    设置当前组的 ID 和 entries_read 计数器。",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(opt,"CREATE") && (c->argc >= 5 && c->argc <= 8)) {
        streamID id;
        if (!strcmp(c->argv[4]->ptr,"$")) {
            if (s) {
                id = s->last_id;
            } else {
                id.ms = 0;
                id.seq = 0;
            }
        } else if (streamParseStrictIDOrReply(c,c->argv[4],&id,0,NULL) != C_OK) {
            return;
        }

        /* Handle the MKSTREAM option now that the command can no longer fail. */
        if (s == NULL) {
            serverAssert(mkstream);
            o = createStreamObject();
            dbAdd(c->db,c->argv[2],o);
            s = o->ptr;
            signalModifiedKey(c,c->db,c->argv[2]);
        }

        streamCG *cg = streamCreateCG(s,grpname,sdslen(grpname),&id,entries_read);
        if (cg) {
            addReply(c,shared.ok);
            server.dirty++;
            notifyKeyspaceEvent(NOTIFY_STREAM,"xgroup-create",
                                c->argv[2],c->db->id);
        } else {
            addReplyError(c,"-BUSYGROUP 消费者组名称已存在");
        }
    } else if (!strcasecmp(opt,"SETID") && (c->argc == 5 || c->argc == 7)) {
        streamID id;
        if (!strcmp(c->argv[4]->ptr,"$")) {
            id = s->last_id;
        } else if (streamParseIDOrReply(c,c->argv[4],&id,0) != C_OK) {
            return;
        }
        cg->last_id = id;
        cg->entries_read = entries_read;
        addReply(c,shared.ok);
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_STREAM,"xgroup-setid",c->argv[2],c->db->id);
    } else if (!strcasecmp(opt,"DESTROY") && c->argc == 4) {
        if (cg) {
            raxRemove(s->cgroups,(unsigned char*)grpname,sdslen(grpname),NULL);
            streamFreeCG(cg);
            addReply(c,shared.cone);
            server.dirty++;
            notifyKeyspaceEvent(NOTIFY_STREAM,"xgroup-destroy",
                                c->argv[2],c->db->id);
            /* 我们希望解除任何因 -NOGROUP 而阻塞的 XREADGROUP 消费者。*/
            signalKeyAsReady(c->db,c->argv[2],OBJ_STREAM);
        } else {
            addReply(c,shared.czero);
        }
    } else if (!strcasecmp(opt,"CREATECONSUMER") && c->argc == 5) {
        streamConsumer *created = streamCreateConsumer(cg,c->argv[4]->ptr,c->argv[2],
                                                       c->db->id,SCC_DEFAULT);
        addReplyLongLong(c,created ? 1 : 0);
    } else if (!strcasecmp(opt,"DELCONSUMER") && c->argc == 5) {
        long long pending = 0;
        streamConsumer *consumer = streamLookupConsumer(cg,c->argv[4]->ptr);
        if (consumer) {
            /* 删除消费者并返回仍与此消费者关联的待处理消息数。*/
            pending = raxSize(consumer->pel);
            streamDelConsumer(cg,consumer);
            server.dirty++;
            notifyKeyspaceEvent(NOTIFY_STREAM,"xgroup-delconsumer",
                                c->argv[2],c->db->id);
        }
        addReplyLongLong(c,pending);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* XSETID <stream> <id> [ENTRIESADDED entries_added] [MAXDELETEDID max_deleted_entry_id]
 *
 * 设置 stream 的内部 "last ID"、"added entries" 和 "maximal deleted entry ID"。*/
void xsetidCommand(client *c) {
    streamID id, max_xdel_id = {0, 0};
    long long entries_added = -1;

    if (streamParseStrictIDOrReply(c,c->argv[2],&id,0,NULL) != C_OK)
        return;

    int i = 3;
    while (i < c->argc) {
        int moreargs = (c->argc-1) - i; /* 剩余参数个数。*/
        char *opt = c->argv[i]->ptr;
        if (!strcasecmp(opt,"ENTRIESADDED") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[i+1],&entries_added,NULL) != C_OK) {
                return;
            } else if (entries_added < 0) {
                addReplyError(c,"entries_added 必须为正数");
                return;
            }
            i += 2;
        } else if (!strcasecmp(opt,"MAXDELETEDID") && moreargs) {
            if (streamParseStrictIDOrReply(c,c->argv[i+1],&max_xdel_id,0,NULL) != C_OK) {
                return;
            } else if (streamCompareID(&id,&max_xdel_id) < 0) {
                addReplyError(c,"XSETID 中指定的 ID 小于提供的 max_deleted_entry_id");
                return;
            }
            i += 2;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_STREAM)) return;
    stream *s = o->ptr;

    if (streamCompareID(&id,&s->max_deleted_entry_id) < 0) {
        addReplyError(c,"XSETID 中指定的 ID 小于当前的 max_deleted_entry_id");
        return;
    }

    /* 如果 stream 至少有一个条目，我们希望检查用户是否正在设置
     * 一个大于或等于当前顶部条目的 last ID，
     * 否则将违反 ID 单调性的基本假设。*/
    if (s->length > 0) {
        streamID maxid;
        streamLastValidID(s,&maxid);

        if (streamCompareID(&id,&maxid) < 0) {
            addReplyError(c,"XSETID 中指定的 ID 小于目标 stream 的顶部条目");
            return;
        }

        /* 如果提供了 entries_added，它不能小于 length。*/
        if (entries_added != -1 && s->length > (uint64_t)entries_added) {
            addReplyError(c,"XSETID 中指定的 entries_added 小于目标 stream 的长度");
            return;
        }
    }

    s->last_id = id;
    if (entries_added != -1)
        s->entries_added = entries_added;
    if (!streamIDEqZero(&max_xdel_id))
        s->max_deleted_entry_id = max_xdel_id;
    addReply(c,shared.ok);
    server.dirty++;
    notifyKeyspaceEvent(NOTIFY_STREAM,"xsetid",c->argv[1],c->db->id);
}

/* XACK <key> <group> <id> <id> ... <id>
 * 确认消息已处理。实际上我们只是检查组的待处理条目列表 (PEL)，
 * 并从组和消费者中删除 PEL 条目（待处理消息在两处都被引用）。
 *
 * 命令的返回值是已成功确认的消息数，即我们实际上能够在 PEL 中解析的 ID 数量。
 */
void xackCommand(client *c) {
    streamCG *group = NULL;
    robj *o = lookupKeyRead(c->db,c->argv[1]);
    if (o) {
        if (checkType(c,o,OBJ_STREAM)) return; /* 类型错误。*/
        group = streamLookupCG(o->ptr,c->argv[2]->ptr);
    }

    /* 没有 key 或组？无需确认。*/
    if (o == NULL || group == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* 尽快开始解析 ID，以便在出现语法错误时立即中止：
     * 该命令的返回值在客户端成功确认一些消息的情况下不能是错误，
     * 因此应以 "全有或全无" 的方式执行。*/
    streamID static_ids[STREAMID_STATIC_VECTOR_LEN];
    streamID *ids = static_ids;
    int id_count = c->argc-3;
    if (id_count > STREAMID_STATIC_VECTOR_LEN)
        ids = zmalloc(sizeof(streamID)*id_count);
    for (int j = 3; j < c->argc; j++) {
        if (streamParseStrictIDOrReply(c,c->argv[j],&ids[j-3],0,NULL) != C_OK) goto cleanup;
    }

    int acknowledged = 0;
    for (int j = 3; j < c->argc; j++) {
        unsigned char buf[sizeof(streamID)];
        streamEncodeID(buf,&ids[j-3]);

        /* 在组 PEL 中查找 ID：它将具有对 NACK 结构的引用，
         * 该 NACK 结构将引用消费者，以便我们能够从两个 PEL 中删除该条目。*/
        void *result;
        if (raxFind(group->pel,buf,sizeof(buf),&result)) {
            streamNACK *nack = result;
            raxRemove(group->pel,buf,sizeof(buf),NULL);
            raxRemove(nack->consumer->pel,buf,sizeof(buf),NULL);
            streamFreeNACK(nack);
            acknowledged++;
            server.dirty++;
        }
    }
    addReplyLongLong(c,acknowledged);
cleanup:
    if (ids != static_ids) zfree(ids);
}

/* XPENDING <key> <group> [[IDLE <idle>] <start> <stop> <count> [<consumer>]]
 *
 * If start and stop are omitted, the command just outputs information about
 * the amount of pending messages for the key/group pair, together with
 * the minimum and maximum ID of pending messages.
 *
 * 如果提供了 start 和 stop，则返回待处理消息及其当前所有者、
 * 投递次数和最后投递时间等信息。*/
void xpendingCommand(client *c) {
    int justinfo = c->argc == 3; /* 没有范围时，仅输出关于 PEL 的一般信息。*/
    robj *key = c->argv[1];
    robj *groupname = c->argv[2];
    robj *consumername = NULL;
    streamID startid, endid;
    long long count = 0;
    long long minidle = 0;
    int startex = 0, endex = 0;

    /* start 和 stop 以及 consumer 可以省略。IDLE 修饰符也是如此。*/
    if (c->argc != 3 && (c->argc < 6 || c->argc > 9)) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 尽快解析 start/end/count 参数，以便在任何其他错误之前报告语法错误。*/
    if (c->argc >= 6) {
        int startidx = 3; /* 不含 IDLE */

        if (!strcasecmp(c->argv[3]->ptr, "IDLE")) {
            if (getLongLongFromObjectOrReply(c, c->argv[4], &minidle, NULL) == C_ERR)
                return;
            if (c->argc < 8) {
                /* 如果提供了 IDLE，则必须至少有 'start end count' */
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
            /* 在 'IDLE <idle>' 之后搜索其余参数 */
            startidx += 2;
        }

        /* count 参数。*/
        if (getLongLongFromObjectOrReply(c,c->argv[startidx+2],&count,NULL) == C_ERR)
            return;
        if (count < 0) count = 0;

        /* start 和 end 参数。*/
        if (streamParseIntervalIDOrReply(c,c->argv[startidx],&startid,&startex,0) != C_OK)
            return;
        if (startex && streamIncrID(&startid) != C_OK) {
            addReplyError(c,"区间的无效 start ID");
            return;
        }
        if (streamParseIntervalIDOrReply(c,c->argv[startidx+1],&endid,&endex,UINT64_MAX) != C_OK)
            return;
        if (endex && streamDecrID(&endid) != C_OK) {
            addReplyError(c,"区间的无效 end ID");
            return;
        }

        if (startidx+3 < c->argc) {
            /* 已提供 'consumer' */
            consumername = c->argv[startidx+3];
        }
    }

    /* 在 stream 中查找 key 和组。*/
    robj *o = lookupKeyRead(c->db,c->argv[1]);
    streamCG *group;

    if (checkType(c,o,OBJ_STREAM)) return;
    if (o == NULL ||
        (group = streamLookupCG(o->ptr,groupname->ptr)) == NULL)
    {
        addReplyErrorFormat(c, "-NOGROUP 不存在 key '%s' 或消费者组 '%s'",
                               (char*)key->ptr,(char*)groupname->ptr);
        return;
    }

    /* XPENDING <key> <group> 变体。*/
    if (justinfo) {
        addReplyArrayLen(c,4);
        /* PEL 中的消息总数。*/
        addReplyLongLong(c,raxSize(group->pel));
        /* 第一个和最后一个 ID。*/
        if (raxSize(group->pel) == 0) {
            addReplyNull(c); /* Start。*/
            addReplyNull(c); /* End。*/
            addReplyNullArray(c); /* Clients。*/
        } else {
            /* Start。*/
            raxIterator ri;
            raxStart(&ri,group->pel);
            raxSeek(&ri,"^",NULL,0);
            raxNext(&ri);
            streamDecodeID(ri.key,&startid);
            addReplyStreamID(c,&startid);

            /* End。*/
            raxSeek(&ri,"$",NULL,0);
            raxNext(&ri);
            streamDecodeID(ri.key,&endid);
            addReplyStreamID(c,&endid);
            raxStop(&ri);

            /* 具有待处理消息的消费者。*/
            raxStart(&ri,group->consumers);
            raxSeek(&ri,"^",NULL,0);
            void *arraylen_ptr = addReplyDeferredLen(c);
            size_t arraylen = 0;
            while(raxNext(&ri)) {
                streamConsumer *consumer = ri.data;
                if (raxSize(consumer->pel) == 0) continue;
                addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c,ri.key,ri.key_len);
                addReplyBulkLongLong(c,raxSize(consumer->pel));
                arraylen++;
            }
            setDeferredArrayLen(c,arraylen_ptr,arraylen);
            raxStop(&ri);
        }
    } else { /* 提供了 <start>, <stop> 和 <count>，返回实际的待处理条目（不仅仅是信息） */
        streamConsumer *consumer = NULL;
        if (consumername) {
            consumer = streamLookupConsumer(group,consumername->ptr);

            /* 如果提到了消费者名称但它不存在，我们可以直接返回一个空数组。*/
            if (consumer == NULL) {
                addReplyArrayLen(c,0);
                return;
            }
        }

        rax *pel = consumer ? consumer->pel : group->pel;
        unsigned char startkey[sizeof(streamID)];
        unsigned char endkey[sizeof(streamID)];
        raxIterator ri;
        mstime_t now = commandTimeSnapshot();

        streamEncodeID(startkey,&startid);
        streamEncodeID(endkey,&endid);
        raxStart(&ri,pel);
        raxSeek(&ri,">=",startkey,sizeof(startkey));
        void *arraylen_ptr = addReplyDeferredLen(c);
        size_t arraylen = 0;

        while(count && raxNext(&ri) && memcmp(ri.key,endkey,ri.key_len) <= 0) {
            streamNACK *nack = ri.data;

            if (minidle) {
                mstime_t this_idle = now - nack->delivery_time;
                if (this_idle < minidle) continue;
            }

            arraylen++;
            count--;
            addReplyArrayLen(c,4);

            /* 条目 ID。*/
            streamID id;
            streamDecodeID(ri.key,&id);
            addReplyStreamID(c,&id);

            /* 消费者名称。*/
            addReplyBulkCBuffer(c,nack->consumer->name,
                                sdslen(nack->consumer->name));

            /* 自上次投递以来经过的毫秒数。*/
            mstime_t elapsed = now - nack->delivery_time;
            if (elapsed < 0) elapsed = 0;
            addReplyLongLong(c,elapsed);

            /* 投递次数。*/
            addReplyLongLong(c,nack->delivery_count);
        }
        raxStop(&ri);
        setDeferredArrayLen(c,arraylen_ptr,arraylen);
    }
}

/* XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
 *        [IDLE <milliseconds>] [TIME <mstime>] [RETRYCOUNT <count>]
 *        [FORCE] [JUSTID]
 *
 * 更改指定 stream 消费者组待处理条目列表中一个或多个消息的所有权。
 *
 * 如果（在指定的消息 ID 中）某消息 ID 存在，并且其空闲时间
 * 大于或等于 <min-idle-time>，则该消息的新所有者变为指定的 <consumer>。
 * 如果指定的最小空闲时间为 0，则无论空闲时间如何都声明该消息。
 *
 * 所有在待处理条目列表中找不到的消息都将被忽略，但如果使用了
 * FORCE 选项则例外。在这种情况下，我们会在消费者组 PEL 中创建一个
 * NACK（代表尚未确认的消息）条目。
 *
 * 该命令在消费者不存在时会将其创建作为副作用。此外，该命令将
 * 消息的空闲时间重置为 0，即使通过 IDLE 或 TIME 选项，
 * 用户也可以控制新的空闲时间。
 *
 * 末尾的选项可用于指定要设置的待处理消息表示的更多属性：
 *
 * 1. IDLE <ms>:
 *      设置消息的空闲时间（上次投递的时间）。
 *      如果未指定 IDLE，则假定 IDLE 为 0，即时间计数被重置，
 *      因为消息现在有了新的所有者正在尝试处理它。
 *
 * 2. TIME <ms-unix-time>:
 *      这与 IDLE 相同，但不是相对的毫秒数，而是将空闲时间
 *      设置为特定的 unix 时间（以毫秒为单位）。这对于重写
 *      AOF 文件以生成 XCLAIM 命令很有用。
 *
 * 3. RETRYCOUNT <count>:
 *      将重试计数器设置为指定的值。每次消息再次被投递时，
 *      该计数器都会递增。通常 XCLAIM 不会更改此计数器，
 *      它仅在调用 XPENDING 命令时提供给客户端：通过这种方式，
 *      客户端可以检测异常，例如由于某些原因在大量投递尝试后
 *      从未处理的消息。
 *
 * 4. FORCE:
 *      即使某些指定的 ID 尚未在分配给其他客户端的 PEL 中，
 *      也会在 PEL 中创建待处理消息条目。但是消息必须存在于
 *      stream 中，否则不存在的消息 ID 将被忽略。
 *
 * 5. JUSTID:
 *      仅返回成功声明的消息的 ID 数组，而不返回实际的消息。
 *
 * 6. LASTID <id>:
 *      如果当前 last ID 小于提供的 ID，则使用指定的 ID 更新
 *      消费者组的 last ID。这用于复制/AOF，因此当我们从消费者
 *      组读取时，被传播以将所有权交给消费者的 XCLAIM 也将用于
 *      更新组的当前 ID。
 *
 * 该命令返回用户成功声明的消息数组，以便调用者能够了解
 * 它现在负责哪些消息。*/
void xclaimCommand(client *c) {
    streamCG *group = NULL;
    robj *o = lookupKeyRead(c->db,c->argv[1]);
    long long minidle; /* 最小空闲时间参数。*/
    long long retrycount = -1;   /* -1 表示未给出 RETRYCOUNT 选项。*/
    mstime_t deliverytime = -1;  /* -1 表示未给出 IDLE/TIME 选项。*/
    int force = 0;
    int justid = 0;

    if (o) {
        if (checkType(c,o,OBJ_STREAM)) return; /* 类型错误。*/
        group = streamLookupCG(o->ptr,c->argv[2]->ptr);
    }

    /* 没有 key 或组？由于必须创建组，因此发送错误。*/
    if (o == NULL || group == NULL) {
        addReplyErrorFormat(c,"-NOGROUP 不存在 key '%s' 或消费者组 '%s'",
                              (char*)c->argv[1]->ptr,
                              (char*)c->argv[2]->ptr);
        return;
    }

    if (getLongLongFromObjectOrReply(c,c->argv[4],&minidle,
        "XCLAIM 的 min-idle-time 参数无效")
        != C_OK) return;
    if (minidle < 0) minidle = 0;

    /* 尽快开始解析 ID，以便在出现语法错误时立即中止：
     * 该命令的返回值在客户端成功声明某些消息的情况下不能是错误，
     * 因此应以 "全有或全无" 的方式执行。*/
    int j;
    streamID static_ids[STREAMID_STATIC_VECTOR_LEN];
    streamID *ids = static_ids;
    int id_count = c->argc-5;
    if (id_count > STREAMID_STATIC_VECTOR_LEN)
        ids = zmalloc(sizeof(streamID)*id_count);
    for (j = 5; j < c->argc; j++) {
        if (streamParseStrictIDOrReply(NULL,c->argv[j],&ids[j-5],0,NULL) != C_OK) break;
    }
    int last_id_arg = j-1; /* 下次迭代这些 ID 时我们将知道其范围。*/

    /* 如果我们因为某些 ID 无法解析而停止，那么它们可能是尾部的选项。*/
    mstime_t now = commandTimeSnapshot();
    streamID last_id = {0,0};
    int propagate_last_id = 0;
    for (; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j; /* 剩余参数个数。*/
        char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt,"FORCE")) {
            force = 1;
        } else if (!strcasecmp(opt,"JUSTID")) {
            justid = 1;
        } else if (!strcasecmp(opt,"IDLE") && moreargs) {
            j++;
            if (getLongLongFromObjectOrReply(c,c->argv[j],&deliverytime,
                "XCLAIM 的 IDLE 选项参数无效")
                != C_OK) goto cleanup;
            deliverytime = now - deliverytime;
        } else if (!strcasecmp(opt,"TIME") && moreargs) {
            j++;
            if (getLongLongFromObjectOrReply(c,c->argv[j],&deliverytime,
                "XCLAIM 的 TIME 选项参数无效")
                != C_OK) goto cleanup;
        } else if (!strcasecmp(opt,"RETRYCOUNT") && moreargs) {
            j++;
            if (getLongLongFromObjectOrReply(c,c->argv[j],&retrycount,
                "XCLAIM 的 RETRYCOUNT 选项参数无效")
                != C_OK) goto cleanup;
        } else if (!strcasecmp(opt,"LASTID") && moreargs) {
            j++;
            if (streamParseStrictIDOrReply(c,c->argv[j],&last_id,0,NULL) != C_OK) goto cleanup;
        } else {
            addReplyErrorFormat(c,"无法识别的 XCLAIM 选项 '%s'",opt);
            goto cleanup;
        }
    }

    if (streamCompareID(&last_id,&group->last_id) > 0) {
        group->last_id = last_id;
        propagate_last_id = 1;
    }

    if (deliverytime != -1) {
        /* 如果通过 IDLE 或 TIME 传递了投递时间，我们对其做一些合理性检查，
         * 并在值无效时将 deliverytime 设为 now（通常是合理的选择）。
         * 在此处引发错误是不明智的，因为客户端可能会从其本地时间开始
         * 进行一些数学运算来计算空闲时间，而例如计算机时间从我们的角度
         * 来看略微超前，并不是失败的好借口。*/
        if (deliverytime < 0 || deliverytime > now) deliverytime = now;
    } else {
        /* 如果没有传递 IDLE/TIME 选项，我们希望最后投递时间为 now，
         * 这样消息的空闲时间将为零。*/
        deliverytime = now;
    }

    /* 执行实际的声明操作。*/
    streamConsumer *consumer = streamLookupConsumer(group,c->argv[3]->ptr);
    if (consumer == NULL) {
        consumer = streamCreateConsumer(group,c->argv[3]->ptr,c->argv[1],c->db->id,SCC_DEFAULT);
    }
    consumer->seen_time = commandTimeSnapshot();

    void *arraylenptr = addReplyDeferredLen(c);
    size_t arraylen = 0;
    for (int j = 5; j <= last_id_arg; j++) {
        streamID id = ids[j-5];
        unsigned char buf[sizeof(streamID)];
        streamEncodeID(buf,&id);

        /* 在组 PEL 中查找 ID。*/
        void *result = NULL;
        raxFind(group->pel,buf,sizeof(buf),&result);
        streamNACK *nack = result;

        /* 条目必须存在，我们才能将其转移给另一个消费者。*/
        if (!streamEntryExists(o->ptr,&id)) {
            /* 从 PEL 中清除此条目，它已不再存在 */
            if (nack != NULL) {
                /* 传播此更改（我们将删除 NACK）。*/
                streamPropagateXCLAIM(c,c->argv[1],group,c->argv[2],c->argv[j],nack);
                propagate_last_id = 0; /* 将由 XCLAIM 自身传播。*/
                server.dirty++;
                /* 释放 NACK */
                raxRemove(group->pel,buf,sizeof(buf),NULL);
                raxRemove(nack->consumer->pel,buf,sizeof(buf),NULL);
                streamFreeNACK(nack);
            }
            continue;
        }

        /* 如果传递了 FORCE，让我们检查该条目是否至少存在于 stream 中。
         * 在这种情况下，我们将从头开始在 PEL 中创建一个新条目，
         * 以便 XCLAIM 也可以用于在 PEL 中创建条目。
         * 这对于 AOF 和消费组的复制非常有用。*/
        if (force && nack == NULL) {
            /* 创建 NACK。*/
            nack = streamCreateNACK(NULL);
            raxInsert(group->pel,buf,sizeof(buf),nack,NULL);
        }

        if (nack != NULL) {
            /* 我们需要检查此条目是否满足调用者请求的最小空闲时间。
             *
             * 注意，nack 可能由 FORCE 创建，在这种情况下没有预先存在的条目，
             * 因此应忽略 minidle，但在这种情况下 nack->consumer 为 NULL。*/
            if (nack->consumer && minidle) {
                mstime_t this_idle = now - nack->delivery_time;
                if (this_idle < minidle) continue;
            }

            if (nack->consumer != consumer) {
                /* 从旧消费者中移除该条目。
                 * 注意，如果我们由于 FORCE 选项在上面创建了 NACK，
                 * 则 nack->consumer 为 NULL。*/
                if (nack->consumer)
                    raxRemove(nack->consumer->pel,buf,sizeof(buf),NULL);
            }
            nack->delivery_time = deliverytime;
            /* 如果给出了投递尝试计数器，则设置它；
             * 否则除非提供了 JUSTID 选项，否则自动递增 */
            if (retrycount >= 0) {
                nack->delivery_count = retrycount;
            } else if (!justid) {
                nack->delivery_count++;
            }
            if (nack->consumer != consumer) {
                /* 在新消费者的本地 PEL 中添加该条目。*/
                raxInsert(consumer->pel,buf,sizeof(buf),nack,NULL);
                nack->consumer = consumer;
            }
            /* 为此条目发送回复。*/
            if (justid) {
                addReplyStreamID(c,&id);
            } else {
                serverAssert(streamReplyWithRange(c,o->ptr,&id,&id,1,0,NULL,NULL,STREAM_RWR_RAWENTRIES,NULL,NULL) == 1);
            }
            arraylen++;

            consumer->active_time = commandTimeSnapshot();

            /* 传播此更改。*/
            streamPropagateXCLAIM(c,c->argv[1],group,c->argv[2],c->argv[j],nack);
            propagate_last_id = 0; /* 将由 XCLAIM 自身传播。*/
            server.dirty++;
        }
    }
    if (propagate_last_id) {
        streamPropagateGroupID(c,c->argv[1],group,c->argv[2]);
        server.dirty++;
    }
    setDeferredArrayLen(c,arraylenptr,arraylen);
    preventCommandPropagation(c);
cleanup:
    if (ids != static_ids) zfree(ids);
}

/* XAUTOCLAIM <key> <group> <consumer> <min-idle-time> <start> [COUNT <count>] [JUSTID]
 *
 * 更改指定 stream 消费者组待处理条目列表中一个或多个消息的所有权。
 *
 * 对于每个 PEL 条目，如果其空闲时间大于或等于 <min-idle-time>，
 * 则该消息的新所有者变为指定的 <consumer>。
 * 如果指定的最小空闲时间为 0，则无论空闲时间如何都声明该消息。
 *
 * 该命令在消费者不存在时会将其创建作为副作用。此外，该命令将
 * 消息的空闲时间重置为 0。
 *
 * 该命令返回用户成功声明的消息数组，以便调用者能够了解
 * 它现在负责哪些消息。*/
void xautoclaimCommand(client *c) {
    streamCG *group = NULL;
    robj *o = lookupKeyRead(c->db,c->argv[1]);
    long long minidle; /* 最小空闲时间参数，以毫秒为单位。*/
    long count = 100; /* 最大声明条目数。*/
    const unsigned attempts_factor = 10;
    streamID startid;
    int startex;
    int justid = 0;

    /* 尽快解析 idle/start/end/count 参数，以便在任何其他错误之前报告语法错误。*/
    if (getLongLongFromObjectOrReply(c,c->argv[4],&minidle,"XAUTOCLAIM 的 min-idle-time 参数无效") != C_OK)
        return;
    if (minidle < 0) minidle = 0;

    if (streamParseIntervalIDOrReply(c,c->argv[5],&startid,&startex,0) != C_OK)
        return;
    if (startex && streamIncrID(&startid) != C_OK) {
        addReplyError(c,"区间的无效 start ID");
        return;
    }

    int j = 6; /* 选项从 argv[6] 开始 */
    while(j < c->argc) {
        int moreargs = (c->argc-1) - j; /* 剩余参数个数。*/
        char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt,"COUNT") && moreargs) {
            long max_count = LONG_MAX / (max(sizeof(streamID), attempts_factor));
            if (getRangeLongFromObjectOrReply(c,c->argv[j+1],1,max_count,&count,"COUNT 必须 > 0") != C_OK)
                return;
            j++;
        } else if (!strcasecmp(opt,"JUSTID")) {
            justid = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
        j++;
    }

    if (o) {
        if (checkType(c,o,OBJ_STREAM))
            return; /* 类型错误。*/
        group = streamLookupCG(o->ptr,c->argv[2]->ptr);
    }

    /* 没有 key 或组？由于必须创建组，因此发送错误。*/
    if (o == NULL || group == NULL) {
        addReplyErrorFormat(c,"-NOGROUP 不存在 key '%s' 或消费者组 '%s'",
                            (char*)c->argv[1]->ptr,
                            (char*)c->argv[2]->ptr);
        return;
    }

    streamID *deleted_ids = ztrymalloc(count * sizeof(streamID));
    if (!deleted_ids) {
        addReplyError(c, "内存不足，无法分配临时内存，COUNT 过高。");
        return;
    }

    /* 执行实际的声明操作。*/
    streamConsumer *consumer = streamLookupConsumer(group,c->argv[3]->ptr);
    if (consumer == NULL) {
        consumer = streamCreateConsumer(group,c->argv[3]->ptr,c->argv[1],c->db->id,SCC_DEFAULT);
    }
    consumer->seen_time = commandTimeSnapshot();

    long long attempts = count * attempts_factor;

    addReplyArrayLen(c, 3); /* 稍后会添加另一个回复 */
    void *endidptr = addReplyDeferredLen(c); /* reply[0] */
    void *arraylenptr = addReplyDeferredLen(c); /* reply[1] */

    unsigned char startkey[sizeof(streamID)];
    streamEncodeID(startkey,&startid);
    raxIterator ri;
    raxStart(&ri,group->pel);
    raxSeek(&ri,">=",startkey,sizeof(startkey));
    size_t arraylen = 0;
    mstime_t now = commandTimeSnapshot();
    int deleted_id_num = 0;
    while (attempts-- && count && raxNext(&ri)) {
        streamNACK *nack = ri.data;

        streamID id;
        streamDecodeID(ri.key, &id);

        /* 条目必须存在，我们才能将其转移给另一个消费者。*/
        if (!streamEntryExists(o->ptr,&id)) {
            /* 传播此更改（我们将删除 NACK）。*/
            robj *idstr = createObjectFromStreamID(&id);
            streamPropagateXCLAIM(c,c->argv[1],group,c->argv[2],idstr,nack);
            decrRefCount(idstr);
            server.dirty++;
            /* 从 PEL 中清除此条目，它已不再存在 */
            raxRemove(group->pel,ri.key,ri.key_len,NULL);
            raxRemove(nack->consumer->pel,ri.key,ri.key_len,NULL);
            streamFreeNACK(nack);
            /* 稍后记住该 ID */
            deleted_ids[deleted_id_num++] = id;
            raxSeek(&ri,">=",ri.key,ri.key_len);
            count--; /* count 是命令响应大小的限制。*/
            continue;
        }

        if (minidle) {
            mstime_t this_idle = now - nack->delivery_time;
            if (this_idle < minidle)
                continue;
        }

        if (nack->consumer != consumer) {
            /* 从旧消费者中移除该条目。
             * 注意，如果我们由于 FORCE 选项在上面创建了 NACK，
             * 则 nack->consumer 为 NULL。*/
            if (nack->consumer)
                raxRemove(nack->consumer->pel,ri.key,ri.key_len,NULL);
        }

        /* 更新消费者和空闲时间。*/
        nack->delivery_time = now;
        /* 除非提供了 JUSTID 选项，否则递增投递尝试计数器 */
        if (!justid)
            nack->delivery_count++;

        if (nack->consumer != consumer) {
            /* 在新消费者的本地 PEL 中添加该条目。*/
            raxInsert(consumer->pel,ri.key,ri.key_len,nack,NULL);
            nack->consumer = consumer;
        }

        /* 为此条目发送回复。*/
        if (justid) {
            addReplyStreamID(c,&id);
        } else {
            serverAssert(streamReplyWithRange(c,o->ptr,&id,&id,1,0,NULL,NULL,STREAM_RWR_RAWENTRIES,NULL,NULL) == 1);
        }
        arraylen++;
        count--;

        consumer->active_time = commandTimeSnapshot();

        /* 传播此更改。*/
        robj *idstr = createObjectFromStreamID(&id);
        streamPropagateXCLAIM(c,c->argv[1],group,c->argv[2],idstr,nack);
        decrRefCount(idstr);
        server.dirty++;
    }

    /* 我们需要返回下一个条目作为下一次 XAUTOCLAIM 调用的游标 */
    raxNext(&ri);

    streamID endid;
    if (raxEOF(&ri)) {
        endid.ms = endid.seq = 0;
    } else {
        streamDecodeID(ri.key, &endid);
    }
    raxStop(&ri);

    setDeferredArrayLen(c,arraylenptr,arraylen);
    setDeferredReplyStreamID(c,endidptr,&endid);

    addReplyArrayLen(c, deleted_id_num); /* reply[2] */
    for (int i = 0; i < deleted_id_num; i++) {
        addReplyStreamID(c, &deleted_ids[i]);
    }
    zfree(deleted_ids);

    preventCommandPropagation(c);
}

/* XDEL <key> [<ID1> <ID2> ... <IDN>]
 *
 * 从 stream 中删除指定的条目。返回实际删除的项目数，
 * 当某些 ID 不存在时，该数字可能与传入的 ID 数量不同。*/
void xdelCommand(client *c) {
    robj *o;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL
        || checkType(c,o,OBJ_STREAM)) return;
    stream *s = o->ptr;

    /* 我们需要在开始时对传入的 ID 进行合理性检查。
     * 即使不是一个严重问题，但命令因解析到无效 ID 而仅部分执行
     * 也是不好的。*/
    streamID static_ids[STREAMID_STATIC_VECTOR_LEN];
    streamID *ids = static_ids;
    int id_count = c->argc-2;
    if (id_count > STREAMID_STATIC_VECTOR_LEN)
        ids = zmalloc(sizeof(streamID)*id_count);
    for (int j = 2; j < c->argc; j++) {
        if (streamParseStrictIDOrReply(c,c->argv[j],&ids[j-2],0,NULL) != C_OK) goto cleanup;
    }

    /* 实际执行命令。*/
    int deleted = 0;
    int first_entry = 0;
    for (int j = 2; j < c->argc; j++) {
        streamID *id = &ids[j-2];
        if (streamDeleteItem(s,id)) {
            /* 我们想知道 stream 中的第一个条目是否已被删除，
             * 以便稍后设置新的第一个条目。*/
            if (streamCompareID(id,&s->first_id) == 0) {
                first_entry = 1;
            }
            /* 如有必要，更新 stream 的最大墓碑 ID。*/
            if (streamCompareID(id,&s->max_deleted_entry_id) > 0) {
                s->max_deleted_entry_id = *id;
            }
            deleted++;
        };
    }

    /* 更新 stream 的 first ID。*/
    if (deleted) {
        if (s->length == 0) {
            s->first_id.ms = 0;
            s->first_id.seq = 0;
        } else if (first_entry) {
            streamGetEdgeID(s,1,1,&s->first_id);
        }
    }

    /* 如有必要，传播写入。*/
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STREAM,"xdel",c->argv[1],c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
cleanup:
    if (ids != static_ids) zfree(ids);
}

/* 通用形式：XTRIM <key> [... 选项 ...]
 *
 * 选项列表：
 *
 * 修剪策略：
 *
 * MAXLEN [~|=] <count>     -- 修剪 stream，使其长度不超过
 *                             指定的长度。在 count 前使用 ~
 *                             以请求近似修剪（如 XADD MAXLEN 选项）。
 * MINID [~|=] <id>         -- 修剪 stream，使其不包含
 *                             ID 小于 'id' 的条目。在 count 前使用 ~
 *                             以请求近似修剪（如 XADD MINID 选项）。
 *
 * 其他选项：
 *
 * LIMIT <entries>          -- 要修剪的最大条目数。
 *                             0 表示无限制。除非另有指定，否则将其设置为
 *                             默认值 100*server.stream_node_max_entries，
 *                             这是为了保持修剪时间合理。
 *                             仅当提供了 `~` 时才有意义。
 */
void xtrimCommand(client *c) {
    robj *o;

    /* 参数解析。*/
    streamAddTrimArgs parsed_args;
    if (streamParseAddOrTrimArgsOrReply(c, &parsed_args, 0) < 0)
        return; /* streamParseAddOrTrimArgsOrReply 已发送回复。*/

    /* 如果 key 不存在，我们返回零即可，即从 stream 中移除的元素数。*/
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL
        || checkType(c,o,OBJ_STREAM)) return;
    stream *s = o->ptr;

    /* 执行修剪操作。*/
    int64_t deleted = streamTrim(s, &parsed_args);
    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_STREAM,"xtrim",c->argv[1],c->db->id);
        if (parsed_args.approx_trim) {
            /* 如果我们的修剪被限制（通过 LIMIT 或 ~），我们必须
             * 重写相关的 trim 参数，以确保在 AOF 加载或副本中不会出现不一致。
             * It's enough to check only args->approx because there is no
             * way LIMIT is given without the ~ option. */
            streamRewriteApproxSpecifier(c,parsed_args.trim_strategy_arg_idx-1);
            streamRewriteTrimArgument(c,s,parsed_args.trim_strategy,parsed_args.trim_strategy_arg_idx);
        }

        /* 传播写入。*/
        signalModifiedKey(c, c->db,c->argv[1]);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/* xinfoCommand 的辅助函数。
 * 处理 XINFO STREAM 的各种变体 */
void xinfoReplyWithStreamInfo(client *c, stream *s) {
    int full = 1;
    long long count = 10; /* 默认 COUNT 为 10，以避免阻塞服务器 */
    robj **optv = c->argv + 3; /* 选项从 XINFO STREAM <key> 之后开始 */
    int optc = c->argc - 3;

    /* 解析选项。*/
    if (optc == 0) {
        full = 0;
    } else {
        /* 有效选项为 [FULL] 或 [FULL COUNT <count>] */
        if (optc != 1 && optc != 3) {
            addReplySubcommandSyntaxError(c);
            return;
        }

        /* 第一个选项必须是 "FULL" */
        if (strcasecmp(optv[0]->ptr,"full")) {
            addReplySubcommandSyntaxError(c);
            return;
        }

        if (optc == 3) {
            /* 第一个选项必须是 "FULL" */
            if (strcasecmp(optv[1]->ptr,"count")) {
                addReplySubcommandSyntaxError(c);
                return;
            }
            if (getLongLongFromObjectOrReply(c,optv[2],&count,NULL) == C_ERR)
                return;
            if (count < 0) count = 10;
        }
    }

    addReplyMapLen(c,full ? 9 : 10);
    addReplyBulkCString(c,"length");
    addReplyLongLong(c,s->length);
    addReplyBulkCString(c,"radix-tree-keys");
    addReplyLongLong(c,raxSize(s->rax));
    addReplyBulkCString(c,"radix-tree-nodes");
    addReplyLongLong(c,s->rax->numnodes);
    addReplyBulkCString(c,"last-generated-id");
    addReplyStreamID(c,&s->last_id);
    addReplyBulkCString(c,"max-deleted-entry-id");
    addReplyStreamID(c,&s->max_deleted_entry_id);
    addReplyBulkCString(c,"entries-added");
    addReplyLongLong(c,s->entries_added);
    addReplyBulkCString(c,"recorded-first-entry-id");
    addReplyStreamID(c,&s->first_id);

    if (!full) {
        /* XINFO STREAM <key> */

        addReplyBulkCString(c,"groups");
        addReplyLongLong(c,s->cgroups ? raxSize(s->cgroups) : 0);

        /* 为了输出第一个/最后一个条目，我们使用 streamReplyWithRange()。*/
        int emitted;
        streamID start, end;
        start.ms = start.seq = 0;
        end.ms = end.seq = UINT64_MAX;
        addReplyBulkCString(c,"first-entry");
        emitted = streamReplyWithRange(c,s,&start,&end,1,0,NULL,NULL,
                                       STREAM_RWR_RAWENTRIES,NULL,NULL);
        if (!emitted) addReplyNull(c);
        addReplyBulkCString(c,"last-entry");
        emitted = streamReplyWithRange(c,s,&start,&end,1,1,NULL,NULL,
                                       STREAM_RWR_RAWENTRIES,NULL,NULL);
        if (!emitted) addReplyNull(c);
    } else {
        /* XINFO STREAM <key> FULL [COUNT <count>] */

        /* Stream 条目 */
        addReplyBulkCString(c,"entries");
        streamReplyWithRange(c,s,NULL,NULL,count,0,NULL,NULL,0,NULL,NULL);

        /* 消费组 */
        addReplyBulkCString(c,"groups");
        if (s->cgroups == NULL) {
            addReplyArrayLen(c,0);
        } else {
            addReplyArrayLen(c,raxSize(s->cgroups));
            raxIterator ri_cgroups;
            raxStart(&ri_cgroups,s->cgroups);
            raxSeek(&ri_cgroups,"^",NULL,0);
            while(raxNext(&ri_cgroups)) {
                streamCG *cg = ri_cgroups.data;
                addReplyMapLen(c,7);

                /* 名称 */
                addReplyBulkCString(c,"name");
                addReplyBulkCBuffer(c,ri_cgroups.key,ri_cgroups.key_len);

                /* 最后投递 ID */
                addReplyBulkCString(c,"last-delivered-id");
                addReplyStreamID(c,&cg->last_id);

                /* 最后投递 ID 的读取计数器 */
                addReplyBulkCString(c,"entries-read");
                if (cg->entries_read != SCG_INVALID_ENTRIES_READ) {
                    addReplyLongLong(c,cg->entries_read);
                } else {
                    addReplyNull(c);
                }

                /* 组 lag */
                addReplyBulkCString(c,"lag");
                streamReplyWithCGLag(c,s,cg);

                /* 组 PEL 计数 */
                addReplyBulkCString(c,"pel-count");
                addReplyLongLong(c,raxSize(cg->pel));

                /* 组 PEL */
                addReplyBulkCString(c,"pending");
                long long arraylen_cg_pel = 0;
                void *arrayptr_cg_pel = addReplyDeferredLen(c);
                raxIterator ri_cg_pel;
                raxStart(&ri_cg_pel,cg->pel);
                raxSeek(&ri_cg_pel,"^",NULL,0);
                while(raxNext(&ri_cg_pel) && (!count || arraylen_cg_pel < count)) {
                    streamNACK *nack = ri_cg_pel.data;
                    addReplyArrayLen(c,4);

                    /* 条目 ID。*/
                    streamID id;
                    streamDecodeID(ri_cg_pel.key,&id);
                    addReplyStreamID(c,&id);

                    /* 消费者名称。*/
                    serverAssert(nack->consumer); /* valgrind 的断言（避免 NPD） */
                    addReplyBulkCBuffer(c,nack->consumer->name,
                                        sdslen(nack->consumer->name));

                    /* 最后投递时间。*/
                    addReplyLongLong(c,nack->delivery_time);

                    /* 投递次数。*/
                    addReplyLongLong(c,nack->delivery_count);

                    arraylen_cg_pel++;
                }
                setDeferredArrayLen(c,arrayptr_cg_pel,arraylen_cg_pel);
                raxStop(&ri_cg_pel);

                /* 消费者 */
                addReplyBulkCString(c,"consumers");
                addReplyArrayLen(c,raxSize(cg->consumers));
                raxIterator ri_consumers;
                raxStart(&ri_consumers,cg->consumers);
                raxSeek(&ri_consumers,"^",NULL,0);
                while(raxNext(&ri_consumers)) {
                    streamConsumer *consumer = ri_consumers.data;
                    addReplyMapLen(c,5);

                    /* 消费者名称 */
                    addReplyBulkCString(c,"name");
                    addReplyBulkCBuffer(c,consumer->name,sdslen(consumer->name));

                    /* seen-time */
                    addReplyBulkCString(c,"seen-time");
                    addReplyLongLong(c,consumer->seen_time);

                    /* active-time */
                    addReplyBulkCString(c,"active-time");
                    addReplyLongLong(c,consumer->active_time);

                    /* 消费者 PEL 计数 */
                    addReplyBulkCString(c,"pel-count");
                    addReplyLongLong(c,raxSize(consumer->pel));

                    /* 消费者 PEL */
                    addReplyBulkCString(c,"pending");
                    long long arraylen_cpel = 0;
                    void *arrayptr_cpel = addReplyDeferredLen(c);
                    raxIterator ri_cpel;
                    raxStart(&ri_cpel,consumer->pel);
                    raxSeek(&ri_cpel,"^",NULL,0);
                    while(raxNext(&ri_cpel) && (!count || arraylen_cpel < count)) {
                        streamNACK *nack = ri_cpel.data;
                        addReplyArrayLen(c,3);

                        /* 条目 ID。*/
                        streamID id;
                        streamDecodeID(ri_cpel.key,&id);
                        addReplyStreamID(c,&id);

                        /* 最后投递时间。*/
                        addReplyLongLong(c,nack->delivery_time);

                        /* 投递次数。*/
                        addReplyLongLong(c,nack->delivery_count);

                        arraylen_cpel++;
                    }
                    setDeferredArrayLen(c,arrayptr_cpel,arraylen_cpel);
                    raxStop(&ri_cpel);
                }
                raxStop(&ri_consumers);
            }
            raxStop(&ri_cgroups);
        }
    }
}

/* XINFO CONSUMERS <key> <group>
 * XINFO GROUPS <key>
 * XINFO STREAM <key> [FULL [COUNT <count>]]
 * XINFO HELP. */
void xinfoCommand(client *c) {
    stream *s = NULL;
    char *opt;
    robj *key;

    /* HELP 比较特殊。尽快处理它。*/
    if (!strcasecmp(c->argv[1]->ptr,"HELP")) {
        const char *help[] = {
"CONSUMERS <key> <groupname>",
"    显示 <groupname> 的消费者。",
"GROUPS <key>",
"    显示 stream 的消费者组。",
"STREAM <key> [FULL [COUNT <count>]",
"    显示有关 stream 的信息。",
NULL
        };
        addReplyHelp(c, help);
        return;
    }

    /* 除了 HELP 在其他子命令之前处理外，所有子命令的形式都是
     * "<subcommand> <key>"。*/
    opt = c->argv[1]->ptr;
    key = c->argv[2];

    /* 现在查找 key，这是除 HELP 之外的所有子命令共用的。*/
    robj *o = lookupKeyReadOrReply(c,key,shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_STREAM)) return;
    s = o->ptr;

    /* 分发不同的子命令。*/
    if (!strcasecmp(opt,"CONSUMERS") && c->argc == 4) {
        /* XINFO CONSUMERS <key> <group>。*/
        streamCG *cg = streamLookupCG(s,c->argv[3]->ptr);
        if (cg == NULL) {
            addReplyErrorFormat(c, "-NOGROUP key '%s' 上不存在消费者组 '%s'",
                                   (char*)c->argv[3]->ptr, (char*)key->ptr);
            return;
        }

        addReplyArrayLen(c,raxSize(cg->consumers));
        raxIterator ri;
        raxStart(&ri,cg->consumers);
        raxSeek(&ri,"^",NULL,0);
        mstime_t now = commandTimeSnapshot();
        while(raxNext(&ri)) {
            streamConsumer *consumer = ri.data;
            mstime_t inactive = consumer->active_time != -1 ? now - consumer->active_time : consumer->active_time;
            mstime_t idle = now - consumer->seen_time;
            if (idle < 0) idle = 0;

            addReplyMapLen(c,4);
            addReplyBulkCString(c,"name");
            addReplyBulkCBuffer(c,consumer->name,sdslen(consumer->name));
            addReplyBulkCString(c,"pending");
            addReplyLongLong(c,raxSize(consumer->pel));
            addReplyBulkCString(c,"idle");
            addReplyLongLong(c,idle);
            addReplyBulkCString(c,"inactive");
            addReplyLongLong(c,inactive);
        }
        raxStop(&ri);
    } else if (!strcasecmp(opt,"GROUPS") && c->argc == 3) {
        /* XINFO GROUPS <key>。*/
        if (s->cgroups == NULL) {
            addReplyArrayLen(c,0);
            return;
        }

        addReplyArrayLen(c,raxSize(s->cgroups));
        raxIterator ri;
        raxStart(&ri,s->cgroups);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            streamCG *cg = ri.data;
            addReplyMapLen(c,6);
            addReplyBulkCString(c,"name");
            addReplyBulkCBuffer(c,ri.key,ri.key_len);
            addReplyBulkCString(c,"consumers");
            addReplyLongLong(c,raxSize(cg->consumers));
            addReplyBulkCString(c,"pending");
            addReplyLongLong(c,raxSize(cg->pel));
            addReplyBulkCString(c,"last-delivered-id");
            addReplyStreamID(c,&cg->last_id);
            addReplyBulkCString(c,"entries-read");
            if (cg->entries_read != SCG_INVALID_ENTRIES_READ) {
                addReplyLongLong(c,cg->entries_read);
            } else {
                addReplyNull(c);
            }
            addReplyBulkCString(c,"lag");
            streamReplyWithCGLag(c,s,cg);
        }
        raxStop(&ri);
    } else if (!strcasecmp(opt,"STREAM")) {
        /* XINFO STREAM <key> [FULL [COUNT <count>]]。*/
        xinfoReplyWithStreamInfo(c,s);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* 校验 stream listpack 条目结构的完整性。
 * 既包括 listpack 本身的有效性，也包括条目结构是否符合有效的 stream。
 * 有效返回 1，无效返回 0。*/
int streamValidateListpackIntegrity(unsigned char *lp, size_t size, int deep) {
    int valid_record;
    unsigned char *p, *next;

    /* 由于我们不想对所有记录运行两次校验，我们仅对 listpack 头进行
     * listpack 校验，其余的在这里完成。*/
    if (!lpValidateIntegrity(lp, size, 0, NULL, NULL))
        return 0;

    /* 在非深度模式下，我们只校验了 listpack 头（编码大小）*/
    if (!deep) return 1;

    next = p = lpValidateFirst(lp);
    if (!lpValidateNext(lp, &next, size)) return 0;
    if (!p) return 0;

    /* 条目计数 */
    int64_t entry_count = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* deleted */
    int64_t deleted_count = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* num-of-fields */
    int64_t master_fields = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* 字段名 */
    for (int64_t j = 0; j < master_fields; j++) {
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
    }

    /* master 条目的零终止符。*/
    int64_t zero = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record || zero != 0) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    entry_count += deleted_count;
    while (entry_count--) {
        if (!p) return 0;
        int64_t fields = master_fields, extra_fields = 3;
        int64_t flags = lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;

        /* entry id */
        lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
        lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;

        if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) {
            /* num-of-fields */
            fields = lpGetIntegerIfValid(p, &valid_record);
            if (!valid_record) return 0;
            p = next; if (!lpValidateNext(lp, &next, size)) return 0;

            /* 字段名 */
            for (int64_t j = 0; j < fields; j++) {
                p = next; if (!lpValidateNext(lp, &next, size)) return 0;
            }

            extra_fields += fields + 1;
        }

        /* 值 */
        for (int64_t j = 0; j < fields; j++) {
            p = next; if (!lpValidateNext(lp, &next, size)) return 0;
        }

        /* lp-count */
        int64_t lp_count = lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        if (lp_count != fields + extra_fields) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
    }

    if (next)
        return 0;

    return 1;
}
