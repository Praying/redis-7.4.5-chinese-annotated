/*
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-current, Redis Ltd.
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

#include "geo.h"
#include "geohash_helper.h"
#include "debugmacro.h"
#include "pqsort.h"

/* 从 t_zset.c 导出的符号仅供 geo.c 使用，
 * 因为它是 Redis 中唯一需要对 zset 进行深入内省的部分。 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
int zslValueLteMax(double value, zrangespec *spec);

/* ====================================================================
 * 本文件实现以下命令：
 *
 *   - geoadd - 向 geoset 中添加坐标
 *   - georadius - 在 geoset 中按坐标搜索半径范围内的成员
 *   - georadiusbymember - 根据 geoset 中某成员的位置搜索半径范围
 * ==================================================================== */

/* ====================================================================
 * geoArray 实现
 * ==================================================================== */

/* 创建一个新的 geoPoint 数组。 */
geoArray *geoArrayCreate(void) {
    geoArray *ga = zmalloc(sizeof(*ga));
    /* 数组空间在第一次调用 geoArrayAppend() 时分配。 */
    ga->array = NULL;
    ga->buckets = 0;
    ga->used = 0;
    return ga;
}

/* 向 geoArray 追加并填充一个新的条目。 */
geoPoint *geoArrayAppend(geoArray *ga, double *xy, double dist,
                         double score, char *member)
{
    if (ga->used == ga->buckets) {
        ga->buckets = (ga->buckets == 0) ? 8 : ga->buckets*2;
        ga->array = zrealloc(ga->array,sizeof(geoPoint)*ga->buckets);
    }
    geoPoint *gp = ga->array+ga->used;
    gp->longitude = xy[0];  // 经度
    gp->latitude = xy[1];   // 纬度
    gp->dist = dist;        // 距搜索中心的距离
    gp->member = member;    // 成员名
    gp->score = score;      // 对应的 zset 分数（geohash 编码）
    ga->used++;
    return gp;
}

/* 销毁由 geoArrayCreate() 创建的 geoArray。 */
void geoArrayFree(geoArray *ga) {
    size_t i;
    for (i = 0; i < ga->used; i++) sdsfree(ga->array[i].member);
    zfree(ga->array);
    zfree(ga);
}

/* ====================================================================
 * 辅助函数
 * ==================================================================== */
/* 将 52 位 geohash 分数解码为 WGS84 经纬度坐标。
 * 输入：bits - 编码后的 geohash 分数；xy - 输出经纬度数组（[0]=经度, [1]=纬度）。
 * 返回：成功返回 C_OK，失败返回 C_ERR。 */
int decodeGeohash(double bits, double *xy) {
    GeoHashBits hash = { .bits = (uint64_t)bits, .step = GEO_STEP_MAX };
    return geohashDecodeToLongLatWGS84(hash, xy);
}

/* 输入参数辅助函数 */
/* 从两个参数中提取经纬度。
 * 取一个指向纬度参数的指针，然后使用下一个参数作为经度。
 * 解析错误时返回 C_ERR，否则返回 C_OK。 */
int extractLongLatOrReply(client *c, robj **argv, double *xy) {
    int i;
    for (i = 0; i < 2; i++) {
        if (getDoubleFromObjectOrReply(c, argv[i], xy + i, NULL) !=
            C_OK) {
            return C_ERR;
        }
    }
    if (xy[0] < GEO_LONG_MIN || xy[0] > GEO_LONG_MAX ||
        xy[1] < GEO_LAT_MIN  || xy[1] > GEO_LAT_MAX) {
        addReplyErrorFormat(c,
            "-ERR invalid longitude,latitude pair %f,%f\r\n",xy[0],xy[1]);
        return C_ERR;
    }
    return C_OK;
}

/* 输入参数辅助函数 */
/* 从 zset 成员的分数中解码出经纬度。
 * 成功解码返回 C_OK，否则返回 C_ERR。 */
int longLatFromMember(robj *zobj, robj *member, double *xy) {
    double score = 0;

    if (zsetScore(zobj, member->ptr, &score) == C_ERR) return C_ERR;
    if (!decodeGeohash(score, xy)) return C_ERR;
    return C_OK;
}

/* 检查单位参数是否与已知单位之一匹配，并返回到米的换算系数
 * （你需要将米除以该换算系数才能转换为相应的单位）。
 *
 * 如果单位无效，则向客户端报告错误，并返回小于零的值。 */
double extractUnitOrReply(client *c, robj *unit) {
    char *u = unit->ptr;

    if (!strcasecmp(u, "m")) {
        return 1;          // 米：1 米
    } else if (!strcasecmp(u, "km")) {
        return 1000;       // 千米
    } else if (!strcasecmp(u, "ft")) {
        return 0.3048;     // 英尺
    } else if (!strcasecmp(u, "mi")) {
        return 1609.34;    // 英里
    } else {
        addReplyError(c,
            "unsupported unit provided. please use M, KM, FT, MI");
        return -1;
    }
}

/* 输入参数辅助函数。
 * 从 'argv' 开始的两个参数中提取距离，形式为：<数字> <单位>。
 * 返回 C_OK 表示成功，返回 C_ERR 表示失败。
 * *conversion 用于存放将米换算为对应单位的系数。 */
int extractDistanceOrReply(client *c, robj **argv,
                              double *conversion, double *radius) {
    double distance;
    if (getDoubleFromObjectOrReply(c, argv[0], &distance,
                                   "need numeric radius") != C_OK) {
        return C_ERR;
    }

    if (distance < 0) {
        addReplyError(c,"radius cannot be negative");
        return C_ERR;
    }
    if (radius) *radius = distance;

    double to_meters = extractUnitOrReply(c,argv[1]);
    if (to_meters < 0) {
        return C_ERR;
    }

    if (conversion) *conversion = to_meters;
    return C_OK;
}

/* 输入参数辅助函数。
 * 从 'argv' 开始的三个参数中提取高度和宽度，形式为：<数字> <数字> <单位>。
 * 返回 C_OK 表示成功，返回 C_ERR 表示失败。
 * *conversion 用于存放将米换算为对应单位的系数。 */
int extractBoxOrReply(client *c, robj **argv, double *conversion,
                         double *width, double *height) {
    double h, w;
    if ((getDoubleFromObjectOrReply(c, argv[0], &w, "need numeric width") != C_OK) ||
        (getDoubleFromObjectOrReply(c, argv[1], &h, "need numeric height") != C_OK)) {
        return C_ERR;
    }

    if (h < 0 || w < 0) {
        addReplyError(c, "height or width cannot be negative");
        return C_ERR;
    }
    if (height) *height = h;
    if (width) *width = w;

    double to_meters = extractUnitOrReply(c,argv[2]);
    if (to_meters < 0) {
        return C_ERR;
    }

    if (conversion) *conversion = to_meters;
    return C_OK;
}

/* 默认的 addReplyDouble 精度太高。
 * 我们使用本函数返回位置距离。"5.2145 meters away" 比
 * "5.2144992818115 meters away" 更友好。我们在小数点后保留 4 位数字，
 * 这样即使单位是千米，返回的值也足够精确。 */
void addReplyDoubleDistance(client *c, double d) {
    char dbuf[128];
    const int dlen = fixedpoint_d2string(dbuf, sizeof(dbuf), d, 4);
    addReplyBulkCBuffer(c, dbuf, dlen);
}

/* geoGetPointsInRange() 的辅助函数：给定表示点的 sorted set 分数
 * 以及一个 GeoShape，检查该点是否在搜索区域内。
 *
 * shape: 矩形/圆形搜索区域
 * score: 经纬度编码后的版本
 * xy: 输出变量，解码后的经纬度
 * distance: 输出变量，搜索区域中心到该点的距离
 *
 * 返回值：
 *
 * 如果点在搜索区域内，返回 C_OK；如果点在区域外，返回 C_ERR。
 * "*xy" 会被填充为解码后的经纬度。
 * "*distance" 会被填充为搜索区域中心到该点的距离。
 */
int geoWithinShape(GeoShape *shape, double score, double *xy, double *distance) {
    if (!decodeGeohash(score,xy)) return C_ERR; /* 无法解码。 */
    /* 注意 geohashGetDistanceIfInRadiusWGS84() 的参数顺序相反：
     * 先是经度，然后是纬度。 */
    if (shape->type == CIRCULAR_TYPE) {
        // 圆形区域（半径搜索）
        if (!geohashGetDistanceIfInRadiusWGS84(shape->xy[0], shape->xy[1], xy[0], xy[1],
                                               shape->t.radius*shape->conversion, distance))
            return C_ERR;
    } else if (shape->type == RECTANGLE_TYPE) {
        // 矩形区域（盒子搜索）
        if (!geohashGetDistanceIfInRectangle(shape->t.r.width * shape->conversion,
                                             shape->t.r.height * shape->conversion,
                                             shape->xy[0], shape->xy[1], xy[0], xy[1], distance))
            return C_ERR;
    }
    return C_OK;
}

/* 查询 Redis sorted set，提取 'min' 和 'max' 之间的所有元素，
 * 将它们追加到 geoPoint 结构数组 'geoArray' 中。
 * 返回追加到数组中的元素数量。
 *
 * 距离指定 'x'、'y' 坐标超过 'radius' 的元素不会被包含。
 *
 * 本函数能够追加到现有结果集的能力对性能至关重要，
 * 因为按半径查询是通过对 sorted set 进行多次查询来完成的，
 * 之后还需要用 qsort 进行排序。
 * 同样我们需要能够尽快剔除搜索半径之外的点，
 * 以免分配和处理过多不需要的点。 */
int geoGetPointsInRange(robj *zobj, double min, double max, GeoShape *shape, geoArray *ga, unsigned long limit) {
    /* minex 0 = 在范围内包含 min；maxex 1 = 在范围内排除 max */
    /* 也就是说：min <= val < max */
    zrangespec range = { .min = min, .max = max, .minex = 0, .maxex = 1 };
    size_t origincount = ga->used;
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        // 处理 listpack 编码的 zset
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr = NULL;
        unsigned int vlen = 0;
        long long vlong = 0;
        double score = 0;

        if ((eptr = zzlFirstInRange(zl, &range)) == NULL) {
            /* 在我们的 min 处没有元素，没有结果。 */
            return 0;
        }

        sptr = lpNext(zl, eptr);
        while (eptr) {
            double xy[2];
            double distance = 0;
            score = zzlGetScore(sptr);

            /* 如果已经超出范围，则跳出循环。 */
            if (!zslValueLteMax(score, &range))
                break;

            vstr = lpGetValue(eptr, &vlen, &vlong);
            if (geoWithinShape(shape, score, xy, &distance) == C_OK) {
                /* 追加新元素。 */
                char *member = (vstr == NULL) ? sdsfromlonglong(vlong) : sdsnewlen(vstr, vlen);
                geoArrayAppend(ga, xy, distance, score, member);
            }
            if (ga->used && limit && ga->used >= limit) break;
            zzlNext(zl, &eptr, &sptr);
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // 处理 skiplist 编码的 zset
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        if ((ln = zslNthInRange(zsl, &range, 0)) == NULL) {
            /* 在我们的 min 处没有元素，没有结果。 */
            return 0;
        }

        while (ln) {
            double xy[2];
            double distance = 0;
            /* 当节点不再在范围内时中止。 */
            if (!zslValueLteMax(ln->score, &range))
                break;
            if (geoWithinShape(shape, ln->score, xy, &distance) == C_OK) {
                /* 追加新元素。 */
                geoArrayAppend(ga, xy, distance, ln->score, sdsdup(ln->ele));
            }
            if (ga->used && limit && ga->used >= limit) break;
            ln = ln->level[0].forward;
        }
    }
    return ga->used - origincount;
}

/* 计算应查询的 sorted set 分数范围 min（含）、max（不含），
 * 以便检索出指定区域 'hash' 内的所有元素。
 * 这两个分数通过引用在 *min 和 *max 中返回。 */
void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
    /* 我们希望计算 sorted set 分数，使其包含指定 Geohash 'hash' 内的所有元素，
     * 该 hash 的位数为 hash.step * 2。
     *
     * 例如，若 step = 3，hash 的二进制值为 101010，
     * 由于我们的分数为 52 位，我们想要的每个元素的二进制形式为：
     * 101010?????????????????????????????????????????????
     * 其中 ? 可以是 0 或 1。
     *
     * 为了得到 min 分数，我们只需要将初始 hash 值左移到
     * 52 位即可。然后我们递增 6 位前缀（见 hash.bits++ 语句），
     * 获得新的前缀：101011，再将其对齐到 52 位以获得最大值
     * （该值在搜索中是被排除的）。于是我们得到下面两个分数
     * （以二进制表示）之间的所有元素：
     *
     * 1010100000000000000000000000000000000000000000000000（包含）
     * 与
     * 1010110000000000000000000000000000000000000000000000（排除）。
     */
    *min = geohashAlign52Bits(hash);
    hash.bits++;
    *max = geohashAlign52Bits(hash);
}

/* 获取此 geohash 包围盒 min/max 之间的所有成员。
 * 通过调用 geoGetPointsInRange() 将结果填充到 geoArray 的 GeoPoint 中。
 * 返回添加到数组中的点的数量。 */
int membersOfGeoHashBox(robj *zobj, GeoHashBits hash, geoArray *ga, GeoShape *shape, unsigned long limit) {
    GeoHashFix52Bits min, max;

    scoresOfGeoHashBox(hash,&min,&max);
    return geoGetPointsInRange(zobj, min, max, shape, ga, limit);
}

/* 搜索自身及八个相邻的 geohash 包围盒 */
int membersOfAllNeighbors(robj *zobj, const GeoHashRadius *n, GeoShape *shape, geoArray *ga, unsigned long limit) {
    GeoHashBits neighbors[9];
    unsigned int i, count = 0, last_processed = 0;
    int debugmsg = 0;

    neighbors[0] = n->hash;              // 自身
    neighbors[1] = n->neighbors.north;   // 北
    neighbors[2] = n->neighbors.south;   // 南
    neighbors[3] = n->neighbors.east;    // 东
    neighbors[4] = n->neighbors.west;    // 西
    neighbors[5] = n->neighbors.north_east;  // 东北
    neighbors[6] = n->neighbors.north_west;  // 西北
    neighbors[7] = n->neighbors.south_east;  // 东南
    neighbors[8] = n->neighbors.south_west;  // 西南

    /* 对每个相邻区域（*以及* 自身的 hashbox），
     * 获取所有匹配的成员并将它们添加到候选结果列表中。 */
    for (i = 0; i < sizeof(neighbors) / sizeof(*neighbors); i++) {
        if (HASHISZERO(neighbors[i])) {
            if (debugmsg) D("neighbors[%d] is zero",i);
            continue;
        }

        /* 调试信息。 */
        if (debugmsg) {
            GeoHashRange long_range, lat_range;
            geohashGetCoordRange(&long_range,&lat_range);
            GeoHashArea myarea = {{0}};
            geohashDecode(long_range, lat_range, neighbors[i], &myarea);

            /* 输出中心方块。 */
            D("neighbors[%d]:\n",i);
            D("area.longitude.min: %f\n", myarea.longitude.min);
            D("area.longitude.max: %f\n", myarea.longitude.max);
            D("area.latitude.min: %f\n", myarea.latitude.min);
            D("area.latitude.max: %f\n", myarea.latitude.max);
            D("\n");
        }

        /* 当使用很大的半径（5000 公里或更大）时，
         * 相邻的区域可能是相同的，从而导致元素重复。
         * 跳过与上一次处理的范围相同的范围。 */
        if (last_processed &&
            neighbors[i].bits == neighbors[last_processed].bits &&
            neighbors[i].step == neighbors[last_processed].step)
        {
            if (debugmsg)
                D("Skipping processing of %d, same as previous\n",i);
            continue;
        }
        if (ga->used && limit && ga->used >= limit) break;
        count += membersOfGeoHashBox(zobj, neighbors[i], ga, shape, limit);
        last_processed = i;
    }
    return count;
}

/* qsort() 的比较器 */
/* 按距离升序排列的比较器 */
static int sort_gp_asc(const void *a, const void *b) {
    const struct geoPoint *gpa = a, *gpb = b;
    /* 我们不能直接做 adist - bdist，因为它们是 double 类型，
     * 而比较器返回 int。 */
    if (gpa->dist > gpb->dist)
        return 1;
    else if (gpa->dist == gpb->dist)
        return 0;
    else
        return -1;
}

/* 按距离降序排列的比较器 */
static int sort_gp_desc(const void *a, const void *b) {
    return -sort_gp_asc(a, b);
}

/* ====================================================================
 * 命令
 * ==================================================================== */

/* GEOADD key [CH] [NX|XX] long lat name [long2 lat2 name2 ... longN latN nameN]
 *
 * 向地理空间索引添加一个或多个位置点（经度、纬度、成员名）。
 * 实际通过将经纬度编码为 geohash 52 位分数后调用 ZADD 实现。
 * 返回新添加的位置点数量（已存在则更新分数）。
 * 时间复杂度：每对 (long, lat, name) 为 O(log(N))。 */
void geoaddCommand(client *c) {
    int xx = 0, nx = 0, longidx = 2;
    int i;

    /* 解析选项。解析结束后 'longidx' 指向第一个元素的经度所在参数位置。 */
    while (longidx < c->argc) {
        char *opt = c->argv[longidx]->ptr;
        if (!strcasecmp(opt,"nx")) nx = 1;
        else if (!strcasecmp(opt,"xx")) xx = 1;
        else if (!strcasecmp(opt,"ch")) { /* 在 zaddCommand 中处理。 */ }
        else break;
        longidx++;
    }

    if ((c->argc - longidx) % 3 || (xx && nx)) {
        /* 到这里之后还需要奇数个参数…… */
            addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 构造用于调用 ZADD 的参数向量。 */
    int elements = (c->argc - longidx) / 3;
    int argc = longidx+elements*2; /* ZADD key [CH] [NX|XX] score ele ... */
    robj **argv = zcalloc(argc*sizeof(robj*));
    argv[0] = createRawStringObject("zadd",4);
    for (i = 1; i < longidx; i++) {
        argv[i] = c->argv[i];
        incrRefCount(argv[i]);
    }

    /* 创建调用 ZADD 的参数向量，以便向目标 zset 添加所有 score,value 对，
     * 其中 score 实际上是 lat,long 的编码版本。 */
    for (i = 0; i < elements; i++) {
        double xy[2];

        if (extractLongLatOrReply(c, (c->argv+longidx)+(i*3),xy) == C_ERR) {
            for (i = 0; i < argc; i++)
                if (argv[i]) decrRefCount(argv[i]);
            zfree(argv);
            return;
        }

        /* 将经纬度转换为元素的分数。 */
        GeoHashBits hash;
        geohashEncodeWGS84(xy[0], xy[1], GEO_STEP_MAX, &hash);
        GeoHashFix52Bits bits = geohashAlign52Bits(hash);
        robj *score = createStringObjectFromLongLongWithSds(bits);
        robj *val = c->argv[longidx + i * 3 + 2];
        argv[longidx+i*2] = score;
        argv[longidx+1+i*2] = val;
        incrRefCount(val);
    }

    /* 最后调用 ZADD，让它为我们完成实际工作。 */
    replaceClientCommandVector(c,argc,argv);
    zaddCommand(c);
}

#define SORT_NONE 0    /* 不排序 */
#define SORT_ASC 1      /* 升序 */
#define SORT_DESC 2     /* 降序 */

#define RADIUS_COORDS (1<<0)    /* 围绕坐标搜索（GEORADIUS）。 */
#define RADIUS_MEMBER (1<<1)    /* 围绕成员搜索（GEORADIUSBYMEMBER）。 */
#define RADIUS_NOSTORE (1<<2)   /* 不接受 STORE/STOREDIST 选项（RO 变体）。 */
#define GEOSEARCH (1<<3)        /* GEOSEARCH 命令变体（支持不同的参数） */
#define GEOSEARCHSTORE (1<<4)   /* GEOSEARCHSTORE 仅接受 STOREDIST 选项 */

/* GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                               [COUNT count [ANY]] [STORE key|STOREDIST key]
 * GEORADIUSBYMEMBER key member radius unit ... options ...
 * GEOSEARCH key [FROMMEMBER member] [FROMLONLAT long lat] [BYRADIUS radius unit]
 *               [BYBOX width height unit] [WITHCOORD] [WITHDIST] [WITHASH] [COUNT count [ANY]] [ASC|DESC]
 * GEOSEARCHSTORE dest_key src_key [FROMMEMBER member] [FROMLONLAT long lat] [BYRADIUS radius unit]
 *               [BYBOX width height unit] [COUNT count [ANY]] [ASC|DESC] [STOREDIST]
 *
 * GEORADIUS/GEORADIUSBYMEMBER/GEOSEARCH/GEOSEARCHSTORE 的通用实现。
 * 负责解析参数、构造搜索形状、查询 zset、排序、返回结果或存储到目标 key。
 * 时间复杂度：O(N+log(M))，其中 N 是中心点附近 geohash 邻域中的元素数，
 * M 是结果数。 */
void georadiusGeneric(client *c, int srcKeyIndex, int flags) {
    robj *storekey = NULL;
    int storedist = 0; /* 0 表示 STORE，1 表示 STOREDIST。 */

    /* 查找请求的 zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[srcKeyIndex]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* 根据查询类型找到用于半径或盒子搜索的经纬度 */
    int base_args;
    GeoShape shape = {0};
    if (flags & RADIUS_COORDS) {
        /* GEORADIUS 或 GEORADIUS_RO */
        base_args = 6;
        shape.type = CIRCULAR_TYPE;
        if (extractLongLatOrReply(c, c->argv + 2, shape.xy) == C_ERR) return;
        if (extractDistanceOrReply(c, c->argv+base_args-2, &shape.conversion, &shape.t.radius) != C_OK) return;
    } else if ((flags & RADIUS_MEMBER) && !zobj) {
        /* 没有源 key，但我们需要继续解析参数，
         * 这样我们才能根据 STORE 标志知道使用哪种响应。 */
        base_args = 5;
    } else if (flags & RADIUS_MEMBER) {
        /* GEORADIUSBYMEMBER 或 GEORADIUSBYMEMBER_RO */
        base_args = 5;
        shape.type = CIRCULAR_TYPE;
        robj *member = c->argv[2];
        if (longLatFromMember(zobj, member, shape.xy) == C_ERR) {
            addReplyError(c, "could not decode requested zset member");
            return;
        }
        if (extractDistanceOrReply(c, c->argv+base_args-2, &shape.conversion, &shape.t.radius) != C_OK) return;
    } else if (flags & GEOSEARCH) {
        /* GEOSEARCH 或 GEOSEARCHSTORE */
        base_args = 2;
        if (flags & GEOSEARCHSTORE) {
            base_args = 3;
            storekey = c->argv[1];
        }
    } else {
        addReplyError(c, "Unknown georadius search type");
        return;
    }

    /* 发现并填充所有可选参数。 */
    int withdist = 0, withhash = 0, withcoords = 0;
    int frommember = 0, fromloc = 0, byradius = 0, bybox = 0;
    int sort = SORT_NONE;
    int any = 0; /* any=1 表示有限搜索，一旦找到足够结果就停止。 */
    long long count = 0;  /* 返回结果的最大数量。0 表示无限制。 */
    if (c->argc > base_args) {
        int remaining = c->argc - base_args;
        for (int i = 0; i < remaining; i++) {
            char *arg = c->argv[base_args + i]->ptr;
            if (!strcasecmp(arg, "withdist")) {
                // 同时返回距离
                withdist = 1;
            } else if (!strcasecmp(arg, "withhash")) {
                // 同时返回 geohash 整数
                withhash = 1;
            } else if (!strcasecmp(arg, "withcoord")) {
                // 同时返回经纬度坐标
                withcoords = 1;
            } else if (!strcasecmp(arg, "any")) {
                // 配合 COUNT 使用：尽快返回足够的结果
                any = 1;
            } else if (!strcasecmp(arg, "asc")) {
                sort = SORT_ASC;
            } else if (!strcasecmp(arg, "desc")) {
                sort = SORT_DESC;
            } else if (!strcasecmp(arg, "count") && (i+1) < remaining) {
                if (getLongLongFromObjectOrReply(c, c->argv[base_args+i+1],
                                                 &count, NULL) != C_OK) return;
                if (count <= 0) {
                    addReplyError(c,"COUNT must be > 0");
                    return;
                }
                i++;
            } else if (!strcasecmp(arg, "store") &&
                       (i+1) < remaining &&
                       !(flags & RADIUS_NOSTORE) &&
                       !(flags & GEOSEARCH))
            {
                // STORE key：将结果以原 geohash 分数保存到目标 zset
                storekey = c->argv[base_args+i+1];
                storedist = 0;
                i++;
            } else if (!strcasecmp(arg, "storedist") &&
                       (i+1) < remaining &&
                       !(flags & RADIUS_NOSTORE) &&
                       !(flags & GEOSEARCH))
            {
                // STOREDIST key：将结果以距中心点的距离作为分数保存
                storekey = c->argv[base_args+i+1];
                storedist = 1;
                i++;
            } else if (!strcasecmp(arg, "storedist") &&
                       (flags & GEOSEARCH) &&
                       (flags & GEOSEARCHSTORE))
            {
                // GEOSEARCHSTORE 的 STOREDIST 形式：使用距离作为分数
                storedist = 1;
            } else if (!strcasecmp(arg, "frommember") &&
                      (i+1) < remaining &&
                      flags & GEOSEARCH &&
                      !fromloc)
            {
                /* 没有源 key 时，继续解析参数，待解析完成后返回错误。 */
                if (zobj == NULL) {
                    frommember = 1;
                    i++;
                    continue;
                }

                if (longLatFromMember(zobj, c->argv[base_args+i+1], shape.xy) == C_ERR) {
                    addReplyError(c, "could not decode requested zset member");
                    return;
                }
                frommember = 1;
                i++;
            } else if (!strcasecmp(arg, "fromlonlat") &&
                       (i+2) < remaining &&
                       flags & GEOSEARCH &&
                       !frommember)
            {
                if (extractLongLatOrReply(c, c->argv+base_args+i+1, shape.xy) == C_ERR) return;
                fromloc = 1;
                i += 2;
            } else if (!strcasecmp(arg, "byradius") &&
                       (i+2) < remaining &&
                       flags & GEOSEARCH &&
                       !bybox)
            {
                if (extractDistanceOrReply(c, c->argv+base_args+i+1, &shape.conversion, &shape.t.radius) != C_OK)
                    return;
                shape.type = CIRCULAR_TYPE;
                byradius = 1;
                i += 2;
            } else if (!strcasecmp(arg, "bybox") &&
                       (i+3) < remaining &&
                       flags & GEOSEARCH &&
                       !byradius)
            {
                if (extractBoxOrReply(c, c->argv+base_args+i+1, &shape.conversion, &shape.t.r.width,
                        &shape.t.r.height) != C_OK) return;
                shape.type = RECTANGLE_TYPE;
                bybox = 1;
                i += 3;
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* 拦截与 STORE 和 STOREDIST 不兼容的选项。 */
    if (storekey && (withdist || withhash || withcoords)) {
        addReplyErrorFormat(c,
            "%s is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            flags & GEOSEARCHSTORE? "GEOSEARCHSTORE": "STORE option in GEORADIUS");
        return;
    }

    if ((flags & GEOSEARCH) && !(frommember || fromloc)) {
        addReplyErrorFormat(c,
            "exactly one of FROMMEMBER or FROMLONLAT can be specified for %s",
            (char *)c->argv[0]->ptr);
        return;
    }

    if ((flags & GEOSEARCH) && !(byradius || bybox)) {
        addReplyErrorFormat(c,
            "exactly one of BYRADIUS and BYBOX can be specified for %s",
            (char *)c->argv[0]->ptr);
        return;
    }

    if (any && !count) {
        addReplyError(c, "the ANY argument requires COUNT argument");
        return;
    }

    /* 源 key 不存在时立即返回。 */
    if (zobj == NULL) {
        if (storekey) {
            /* store key 不为空，尝试删除它并返回 0。 */
            if (dbDelete(c->db, storekey)) {
                signalModifiedKey(c, c->db, storekey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", storekey, c->db->id);
                server.dirty++;
            }
            addReply(c, shared.czero);
        } else {
            /* 否则返回一个空数组。 */
            addReply(c, shared.emptyarray);
        }
        return;
    }

    /* 仅使用 COUNT 而不指定排序意义不大（我们需要排序才能返回
     * 最近的 N 个条目），因此如果指定了 COUNT 但未请求排序，
     * 强制使用 ASC 排序。注意 ANY 选项不需要这样处理。 */
    if (count != 0 && sort == SORT_NONE && !any) sort = SORT_ASC;

    /* 获取半径搜索涉及的所有相邻 geohash 包围盒 */
    GeoHashRadius georadius = geohashCalculateAreasByShapeWGS84(&shape);

    /* 在 zset 中搜索所有匹配的点 */
    geoArray *ga = geoArrayCreate();
    membersOfAllNeighbors(zobj, &georadius, &shape, ga, any ? count : 0);

    /* 如果没有匹配结果，用户将收到空响应。 */
    if (ga->used == 0 && storekey == NULL) {
        addReply(c,shared.emptyarray);
        geoArrayFree(ga);
        return;
    }

    long result_length = ga->used;
    long returned_items = (count == 0 || result_length < count) ?
                          result_length : count;
    long option_length = 0;

    /* 处理[可选]的请求排序 */
    if (sort != SORT_NONE) {
        int (*sort_gp_callback)(const void *a, const void *b) = NULL;
        if (sort == SORT_ASC) {
            sort_gp_callback = sort_gp_asc;
        } else if (sort == SORT_DESC) {
            sort_gp_callback = sort_gp_desc;
        }

        if (returned_items == result_length) {
            qsort(ga->array, result_length, sizeof(geoPoint), sort_gp_callback);
        } else {
            // 部分排序：仅对前 returned_items 个元素排序
            pqsort(ga->array, result_length, sizeof(geoPoint), sort_gp_callback,
                0, (returned_items - 1));
        }
    }

    if (storekey == NULL) {
        /* 没有目标 key，将结果返回给用户。 */

        /* 我们的选项是自包含的嵌套 multi-bulk 响应，
         * 因此我们只需要跟踪返回的嵌套响应的数量。 */
        if (withdist)
            option_length++;

        if (withcoords)
            option_length++;

        if (withhash)
            option_length++;

        /* 我们发送的数组长度恰好是 result_length。
         * 结果要么是仅包含 zset 成员字符串的纯字符串数组，
         * 要么是包含 zset 成员字符串_以及_用户为本次请求启用的
         * 所有附加选项的嵌套 multi-bulk 响应。 */
        addReplyArrayLen(c, returned_items);

        /* 最后将结果发回给调用者 */
        int i;
        for (i = 0; i < returned_items; i++) {
            geoPoint *gp = ga->array+i;
            gp->dist /= shape.conversion; /* 根据单位修正。 */

            /* 如果 option_length 不为零，将每个子结果作为
             * 嵌套的 multi-bulk 返回。加 1 是为了计入结果值本身。 */
            if (option_length)
                addReplyArrayLen(c, option_length + 1);

            addReplyBulkSds(c,gp->member);
            gp->member = NULL;

            if (withdist)
                addReplyDoubleDistance(c, gp->dist);

            if (withhash)
                addReplyLongLong(c, gp->score);

            if (withcoords) {
                addReplyArrayLen(c, 2);
                addReplyHumanLongDouble(c, gp->longitude);
                addReplyHumanLongDouble(c, gp->latitude);
            }
        }
    } else {
        /* 有目标 key，使用结果创建 sorted set。 */
        robj *zobj;
        zset *zs;
        int i;
        size_t maxelelen = 0, totelelen = 0;

        if (returned_items) {
            zobj = createZsetObject();
            zs = zobj->ptr;
        }

        for (i = 0; i < returned_items; i++) {
            zskiplistNode *znode;
            geoPoint *gp = ga->array+i;
            gp->dist /= shape.conversion; /* 根据单位修正。 */
            double score = storedist ? gp->dist : gp->score;
            size_t elelen = sdslen(gp->member);

            if (maxelelen < elelen) maxelelen = elelen;
            totelelen += elelen;
            znode = zslInsert(zs->zsl,score,gp->member);
            serverAssert(dictAdd(zs->dict,gp->member,&znode->score) == DICT_OK);
            gp->member = NULL;
        }

        if (returned_items) {
            zsetConvertToListpackIfNeeded(zobj,maxelelen,totelelen);
            setKey(c,c->db,storekey,zobj,0);
            decrRefCount(zobj);
            notifyKeyspaceEvent(NOTIFY_ZSET,flags & GEOSEARCH ? "geosearchstore" : "georadiusstore",storekey,
                                c->db->id);
            server.dirty += returned_items;
        } else if (dbDelete(c->db,storekey)) {
            signalModifiedKey(c,c->db,storekey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",storekey,c->db->id);
            server.dirty++;
        }
        addReplyLongLong(c, returned_items);
    }
    geoArrayFree(ga);
}

/* GEORADIUS 命令包装函数。 */
void georadiusCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_COORDS);
}

/* GEORADIUSBYMEMBER 命令包装函数。 */
void georadiusbymemberCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_MEMBER);
}

/* GEORADIUS_RO 命令包装函数（只读，不支持 STORE/STOREDIST）。 */
void georadiusroCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_COORDS|RADIUS_NOSTORE);
}

/* GEORADIUSBYMEMBER_RO 命令包装函数（只读，不支持 STORE/STOREDIST）。 */
void georadiusbymemberroCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_MEMBER|RADIUS_NOSTORE);
}

/* GEOSEARCH 命令实现。 */
void geosearchCommand(client *c) {
    georadiusGeneric(c, 1, GEOSEARCH);
}

/* GEOSEARCHSTORE 命令实现。 */
void geosearchstoreCommand(client *c) {
    georadiusGeneric(c, 2, GEOSEARCH|GEOSEARCHSTORE);
}

/* GEOHASH key ele1 ele2 ... eleN
 *
 * 返回一个数组，其中包含指定元素位置的 11 字符串 geohash 表示。
 * 缺失的元素使用 NULL bulk 响应填充。
 * 时间复杂度：每个请求元素为 O(log(N))。 */
void geohashCommand(client *c) {
    char *geoalphabet= "0123456789bcdefghjkmnpqrstuvwxyz";
    int j;

    /* 查找请求的 zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* 依次处理每个 geohash 元素，对缺失元素使用 null bulk 响应。 */
    addReplyArrayLen(c,c->argc-2);
    for (j = 2; j < c->argc; j++) {
        double score;
        if (!zobj || zsetScore(zobj, c->argv[j]->ptr, &score) == C_ERR) {
            addReplyNull(c);
        } else {
            /* 我们用于地理编码的内部格式与标准略有不同，
             * 因为我们使用的初始纬度范围是 -85,85，
             * 而标准 geohash 算法使用 -90,90。
             * 因此我们必须解码位置，然后使用标准范围重新编码，
             * 以便输出有效的 geohash 字符串。 */

            /* 解码…… */
            double xy[2];
            if (!decodeGeohash(score,xy)) {
                addReplyNull(c);
                continue;
            }

            /* 重新编码 */
            GeoHashRange r[2];
            GeoHashBits hash;
            r[0].min = -180;
            r[0].max = 180;
            r[1].min = -90;
            r[1].max = 90;
            geohashEncode(&r[0],&r[1],xy[0],xy[1],26,&hash);

            char buf[12];
            int i;
            for (i = 0; i < 11; i++) {
                int idx;
                if (i == 10) {
                    /* 我们只有 52 位，但该 API 历史上输出
                     * 11 字节的 geohash。为了兼容性，我们假设为零。 */
                    idx = 0;
                } else {
                    idx = (hash.bits >> (52-((i+1)*5))) & 0x1f;
                }
                buf[i] = geoalphabet[idx];
            }
            buf[11] = '\0';
            addReplyBulkCBuffer(c,buf,11);
        }
    }
}

/* GEOPOS key ele1 ele2 ... eleN
 *
 * 返回一个数组，其中每个元素是表示参数中指定元素 x,y 位置的二元数组。
 * 缺失的元素返回 NULL。
 * 时间复杂度：每个请求元素为 O(log(N))。 */
void geoposCommand(client *c) {
    int j;

    /* 查找请求的 zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* 依次报告每个元素，对缺失元素使用 null bulk 响应。 */
    addReplyArrayLen(c,c->argc-2);
    for (j = 2; j < c->argc; j++) {
        double score;
        if (!zobj || zsetScore(zobj, c->argv[j]->ptr, &score) == C_ERR) {
            addReplyNullArray(c);
        } else {
            /* 解码…… */
            double xy[2];
            if (!decodeGeohash(score,xy)) {
                addReplyNullArray(c);
                continue;
            }
            addReplyArrayLen(c,2);
            addReplyHumanLongDouble(c,xy[0]);
            addReplyHumanLongDouble(c,xy[1]);
        }
    }
}

/* GEODIST key ele1 ele2 [unit]
 *
 * 返回点 ele1 和 ele2 之间的距离，默认以米为单位，
 * 否则按 "unit" 参数指定的单位返回。
 * 如果其中一个或两个元素不存在则返回 NULL。
 * 时间复杂度：O(log(N))。 */
void geodistCommand(client *c) {
    double to_meter = 1;

    /* 检查是否存在要提取的单位参数，否则默认使用米。 */
    if (c->argc == 5) {
        to_meter = extractUnitOrReply(c,c->argv[4]);
        if (to_meter < 0) return;
    } else if (c->argc > 5) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 查找请求的 zset */
    robj *zobj = NULL;
    if ((zobj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp]))
        == NULL || checkType(c, zobj, OBJ_ZSET)) return;

    /* 获取分数。我们需要同时获得两个分数，否则返回 NULL。 */
    double score1, score2, xyxy[4];
    if (zsetScore(zobj, c->argv[2]->ptr, &score1) == C_ERR ||
        zsetScore(zobj, c->argv[3]->ptr, &score2) == C_ERR)
    {
        addReplyNull(c);
        return;
    }

    /* 解码并计算距离。 */
    if (!decodeGeohash(score1,xyxy) || !decodeGeohash(score2,xyxy+2))
        addReplyNull(c);
    else
        addReplyDoubleDistance(c,
            geohashGetDistance(xyxy[0],xyxy[1],xyxy[2],xyxy[3]) / to_meter);
}
