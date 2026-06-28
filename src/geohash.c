/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-current, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "geohash.h"

/**
 * Geohash 编码原理:
 * 将世界地图划分为 4 个象限，按如下方式标记:
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,1   | 1,1   |
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,0   | 1,0   |
 *  -----------------
 *
 * 其中 x 轴代表经度，y 轴代表纬度。
 * 通过递归细分，可以得到任意精度的地理坐标编码。
 */

/* Interleave lower bits of x and y, so the bits of x
 * are in the even positions and bits from y in the odd;
 * x and y must initially be less than 2**32 (4294967296).
 * From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
 *
 * 该函数实现 Morton Code (Z-order curve) 编码，将二维坐标 (x, y)
 * 交织成一个一维值。x 的位放在偶数位，y 的位放在奇数位。
 * 这使得相近的二维坐标在编码后也相近，便于范围查询。
 */
static inline uint64_t interleave64(uint32_t xlo, uint32_t ylo) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL};
    static const unsigned int S[] = {1, 2, 4, 8, 16};

    uint64_t x = xlo;
    uint64_t y = ylo;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    return x | (y << 1);
}

/* reverse the interleave process
 * derived from http://stackoverflow.com/questions/4909263
 *
 * 反交织过程: 将交织后的 64 位值分离回原始的 x 和 y 坐标。
 * 这是 interleave64 的逆运算。
 */
static inline uint64_t deinterleave64(uint64_t interleaved) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
    static const unsigned int S[] = {0, 1, 2, 4, 8, 16};

    uint64_t x = interleaved;
    uint64_t y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    return x | (y << 32);
}

/* 获取地理坐标的有效范围
 *
 * 这些是 EPSG:900913 / EPSG:3785 / OSGEO:41001 投影的约束。
 * 由于无法在南北极进行地理编码，此处排除了极点区域。
 *
 * @param long_range 输出参数，经度范围
 * @param lat_range  输出参数，纬度范围
 */
void geohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range) {
    /* 这些是 EPSG:900913 / EPSG:3785 / OSGEO:41001 投影的约束 */
    /* 无法在南北极进行地理编码 */
    long_range->max = GEO_LONG_MAX;
    long_range->min = GEO_LONG_MIN;
    lat_range->max = GEO_LAT_MAX;
    lat_range->min = GEO_LAT_MIN;
}

/* 将经纬度坐标编码为 Geohash 值
 *
 * @param long_range 经度范围
 * @param lat_range 纬度范围
 * @param longitude 经度值
 * @param latitude 纬度值
 * @param step     编码精度 (1-32)，表示编码的位数
 * @param hash     输出参数，编码后的 Geohash 位值
 * @return 成功返回 1，失败返回 0
 */
int geohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range,
                  double longitude, double latitude, uint8_t step,
                  GeoHashBits *hash) {
    /* Check basic arguments sanity. */
    if (hash == NULL || step > 32 || step == 0 ||
        RANGEPISZERO(lat_range) || RANGEPISZERO(long_range)) return 0;

    /* Return an error when trying to index outside the supported
     * constraints. */
    if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN ||
        latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) return 0;

    hash->bits = 0;
    hash->step = step;

    if (latitude < lat_range->min || latitude > lat_range->max ||
        longitude < long_range->min || longitude > long_range->max) {
        return 0;
    }

    double lat_offset =
        (latitude - lat_range->min) / (lat_range->max - lat_range->min);
    double long_offset =
        (longitude - long_range->min) / (long_range->max - long_range->min);

    /* convert to fixed point based on the step size */
    lat_offset *= (1ULL << step);
    long_offset *= (1ULL << step);
    hash->bits = interleave64(lat_offset, long_offset);
    return 1;
}

/* 使用默认坐标范围将经纬度编码为 Geohash
 *
 * @param longitude 经度值
 * @param latitude 纬度值
 * @param step     编码精度 (1-32)
 * @param hash     输出参数，编码后的 Geohash 位值
 * @return 成功返回 1，失败返回 0
 */
int geohashEncodeType(double longitude, double latitude, uint8_t step, GeoHashBits *hash) {
    GeoHashRange r[2] = {{0}};
    geohashGetCoordRange(&r[0], &r[1]);
    return geohashEncode(&r[0], &r[1], longitude, latitude, step, hash);
}

/* 使用 WGS84 坐标系将经纬度编码为 Geohash
 *
 * WGS84 是 GPS 使用的标准坐标系。
 *
 * @param longitude 经度值
 * @param latitude 纬度值
 * @param step     编码精度 (1-32)
 * @param hash     输出参数，编码后的 Geohash 位值
 * @return 成功返回 1，失败返回 0
 */
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step,
                       GeoHashBits *hash) {
    return geohashEncodeType(longitude, latitude, step, hash);
}

/* 将 Geohash 值解码为地理区域范围
 *
 * @param long_range 经度范围
 * @param lat_range  纬度范围
 * @param hash       要解码的 Geohash 值
 * @param area       输出参数，解码后的地理区域
 * @return 成功返回 1，失败返回 0
 */
int geohashDecode(const GeoHashRange long_range, const GeoHashRange lat_range,
                   const GeoHashBits hash, GeoHashArea *area) {
    if (HASHISZERO(hash) || NULL == area || RANGEISZERO(lat_range) ||
        RANGEISZERO(long_range)) {
        return 0;
    }

    area->hash = hash;
    uint8_t step = hash.step;
    uint64_t hash_sep = deinterleave64(hash.bits); /* hash = [LAT][LONG] */

    double lat_scale = lat_range.max - lat_range.min;
    double long_scale = long_range.max - long_range.min;

    uint32_t ilato = hash_sep;       /* get lat part of deinterleaved hash */
    uint32_t ilono = hash_sep >> 32; /* shift over to get long part of hash */

    /* divide by 2**step.
     * Then, for 0-1 coordinate, multiply times scale and add
       to the min to get the absolute coordinate. */
    area->latitude.min =
        lat_range.min + (ilato * 1.0 / (1ull << step)) * lat_scale;
    area->latitude.max =
        lat_range.min + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
    area->longitude.min =
        long_range.min + (ilono * 1.0 / (1ull << step)) * long_scale;
    area->longitude.max =
        long_range.min + ((ilono + 1) * 1.0 / (1ull << step)) * long_scale;

    return 1;
}

/* 使用默认坐标范围将 Geohash 解码为地理区域
 *
 * @param hash  要解码的 Geohash 值
 * @param area  输出参数，解码后的地理区域
 * @return 成功返回 1，失败返回 0
 */
int geohashDecodeType(const GeoHashBits hash, GeoHashArea *area) {
    GeoHashRange r[2] = {{0}};
    geohashGetCoordRange(&r[0], &r[1]);
    return geohashDecode(r[0], r[1], hash, area);
}

/* 使用 WGS84 坐标系将 Geohash 解码为地理区域
 *
 * @param hash  要解码的 Geohash 值
 * @param area  输出参数，解码后的地理区域
 * @return 成功返回 1，失败返回 0
 */
int geohashDecodeWGS84(const GeoHashBits hash, GeoHashArea *area) {
    return geohashDecodeType(hash, area);
}

/* 将地理区域解码为中心点经纬度坐标
 *
 * @param area  地理区域
 * @param xy    输出数组，xy[0] 为经度，xy[1] 为纬度
 * @return 成功返回 1，失败返回 0
 */
int geohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy) {
    if (!xy) return 0;
    xy[0] = (area->longitude.min + area->longitude.max) / 2;
    if (xy[0] > GEO_LONG_MAX) xy[0] = GEO_LONG_MAX;
    if (xy[0] < GEO_LONG_MIN) xy[0] = GEO_LONG_MIN;
    xy[1] = (area->latitude.min + area->latitude.max) / 2;
    if (xy[1] > GEO_LAT_MAX) xy[1] = GEO_LAT_MAX;
    if (xy[1] < GEO_LAT_MIN) xy[1] = GEO_LAT_MIN;
    return 1;
}

/* 使用默认坐标范围将 Geohash 解码为中心点坐标
 *
 * @param hash  要解码的 Geohash 值
 * @param xy    输出数组，xy[0] 为经度，xy[1] 为纬度
 * @return 成功返回 1，失败返回 0
 */
int geohashDecodeToLongLatType(const GeoHashBits hash, double *xy) {
    GeoHashArea area = {{0}};
    if (!xy || !geohashDecodeType(hash, &area))
        return 0;
    return geohashDecodeAreaToLongLat(&area, xy);
}

/* 使用 WGS84 坐标系将 Geohash 解码为中心点坐标
 *
 * @param hash  要解码的 Geohash 值
 * @param xy    输出数组，xy[0] 为经度，xy[1] 为纬度
 * @return 成功返回 1，失败返回 0
 */
int geohashDecodeToLongLatWGS84(const GeoHashBits hash, double *xy) {
    return geohashDecodeToLongLatType(hash, xy);
}

/* 沿 X 轴 (经度方向) 移动 Geohash 网格
 *
 * @param hash 输入输出的 Geohash 值
 * @param d    移动方向: 正数向东，负数向西
 */
static void geohash_move_x(GeoHashBits *hash, int8_t d) {
    if (d == 0)
        return;

    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash->bits & 0x5555555555555555ULL;

    uint64_t zz = 0x5555555555555555ULL >> (64 - hash->step * 2);

    if (d > 0) {
        x = x + (zz + 1);
    } else {
        x = x | zz;
        x = x - (zz + 1);
    }

    x &= (0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2));
    hash->bits = (x | y);
}

/* 沿 Y 轴 (纬度方向) 移动 Geohash 网格
 *
 * @param hash 输入输出的 Geohash 值
 * @param d    移动方向: 正数向北，负数向南
 */
static void geohash_move_y(GeoHashBits *hash, int8_t d) {
    if (d == 0)
        return;

    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash->bits & 0x5555555555555555ULL;

    uint64_t zz = 0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2);
    if (d > 0) {
        y = y + (zz + 1);
    } else {
        y = y | zz;
        y = y - (zz + 1);
    }
    y &= (0x5555555555555555ULL >> (64 - hash->step * 2));
    hash->bits = (x | y);
}

/* 计算给定 Geohash 网格的所有相邻网格
 *
 * 返回一个包含 8 个相邻网格的结构:
 *   NW -- N -- NE
 *    |       |
 *   W   C    E
 *    |       |
 *   SW -- S -- SE
 *
 * @param hash      输入的 Geohash 值
 * @param neighbors 输出参数，包含所有相邻网格的结构
 */
void geohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors) {
    neighbors->east = *hash;
    neighbors->west = *hash;
    neighbors->north = *hash;
    neighbors->south = *hash;
    neighbors->south_east = *hash;
    neighbors->south_west = *hash;
    neighbors->north_east = *hash;
    neighbors->north_west = *hash;

    geohash_move_x(&neighbors->east, 1);
    geohash_move_y(&neighbors->east, 0);

    geohash_move_x(&neighbors->west, -1);
    geohash_move_y(&neighbors->west, 0);

    geohash_move_x(&neighbors->south, 0);
    geohash_move_y(&neighbors->south, -1);

    geohash_move_x(&neighbors->north, 0);
    geohash_move_y(&neighbors->north, 1);

    geohash_move_x(&neighbors->north_west, -1);
    geohash_move_y(&neighbors->north_west, 1);

    geohash_move_x(&neighbors->north_east, 1);
    geohash_move_y(&neighbors->north_east, 1);

    geohash_move_x(&neighbors->south_east, 1);
    geohash_move_y(&neighbors->south_east, -1);

    geohash_move_x(&neighbors->south_west, -1);
    geohash_move_y(&neighbors->south_west, -1);
}
