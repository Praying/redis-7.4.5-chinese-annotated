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

/* This is a C++ to C conversion from the ardb project.
 * This file started out as:
 * https://github.com/yinqiwen/ardb/blob/d42503/src/geo/geohash_helper.cpp
 */

#include "fmacros.h"
#include "geohash_helper.h"
#include "debugmacro.h"
#include <math.h>

#define D_R (M_PI / 180.0)
#define R_MAJOR 6378137.0
#define R_MINOR 6356752.3142
#define RATIO (R_MINOR / R_MAJOR)
#define ECCENT (sqrt(1.0 - (RATIO *RATIO)))
#define COM (0.5 * ECCENT)

/// @brief The usual PI/180 constant
const double DEG_TO_RAD = 0.017453292519943295769236907684886;
/// @brief Earth's quatratic mean radius for WGS-84
const double EARTH_RADIUS_IN_METERS = 6372797.560856;

const double MERCATOR_MAX = 20037726.37;
const double MERCATOR_MIN = -20037726.37;

static inline double deg_rad(double ang) { return ang * D_R; }
static inline double rad_deg(double ang) { return ang / D_R; }

/* 根据搜索半径估算 Geohash 精度步长
 *
 * 在半径查询时，估计 9 个搜索区域网格所需的精度位数。
 * 精度越高，网格越小，搜索结果越精确，但需要检查的网格越多。
 *
 * @param range_meters 搜索半径（米）
 * @param lat          当前纬度
 * @return 估算的精度步长 (1-26)
 */
uint8_t geohashEstimateStepsByRadius(double range_meters, double lat) {
    if (range_meters == 0) return 26;
    int step = 1;
    while (range_meters < MERCATOR_MAX) {
        range_meters *= 2;
        step++;
    }
    step -= 2; /* Make sure range is included in most of the base cases. */

    /* Wider range towards the poles... Note: it is possible to do better
     * than this approximation by computing the distance between meridians
     * at this latitude, but this does the trick for now. */
    if (lat > 66 || lat < -66) {
        step--;
        if (lat > 80 || lat < -80) step--;
    }

    /* Frame to valid range. */
    if (step < 1) step = 1;
    if (step > 26) step = 26;
    return step;
}

/* 返回搜索区域的边界框
 *
 * 根据搜索形状 (见 geohash.h GeoShape) 计算边界框。
 * bounds[0] - bounds[2] 是经度的最小值和最大值
 * bounds[1] - bounds[3] 是纬度的最小值和最大值。
 * 由于纬度越高，弧长越短，边界框形状如下
 * (左右边缘实际上是弯曲的)，如下图所示:
 *
 *    \-----------------/          --------               \-----------------/
 *     \               /         /          \              \               /
 *      \  (long,lat) /         / (long,lat) \              \  (long,lat) /
 *       \           /         /              \             /             \
 *         -----          /----------------\           /---------------\
 *  Northern Hemisphere       Southern Hemisphere         Around the equator
 *
 * @param shape  搜索形状 (圆形或矩形)
 * @param bounds 输出数组，存储边界框坐标 [min_lon, min_lat, max_lon, max_lat]
 * @return 成功返回 1，失败返回 0
 */
int geohashBoundingBox(GeoShape *shape, double *bounds) {
    if (!bounds) return 0;
    double longitude = shape->xy[0];
    double latitude = shape->xy[1];
    double height = shape->conversion * (shape->type == CIRCULAR_TYPE ? shape->t.radius : shape->t.r.height/2);
    double width = shape->conversion * (shape->type == CIRCULAR_TYPE ? shape->t.radius : shape->t.r.width/2);

    const double lat_delta = rad_deg(height/EARTH_RADIUS_IN_METERS);
    const double long_delta_top = rad_deg(width/EARTH_RADIUS_IN_METERS/cos(deg_rad(latitude+lat_delta)));
    const double long_delta_bottom = rad_deg(width/EARTH_RADIUS_IN_METERS/cos(deg_rad(latitude-lat_delta)));
    /* The directions of the northern and southern hemispheres
     * are opposite, so we choice different points as min/max long/lat */
    int southern_hemisphere = latitude < 0 ? 1 : 0;
    bounds[0] = southern_hemisphere ? longitude-long_delta_bottom : longitude-long_delta_top;
    bounds[2] = southern_hemisphere ? longitude+long_delta_bottom : longitude+long_delta_top;
    bounds[1] = latitude - lat_delta;
    bounds[3] = latitude + lat_delta;
    return 1;
}

/* 计算能够覆盖指定位置和形状范围内查询的一组区域
 *
 * 计算中心点及其周围 8 个相邻网格，形成 9 个网格来覆盖搜索区域。
 * 搜索形状可以是圆形 (CIRCULAR_TYPE) 或矩形 (RECTANGLE_TYPE)。
 * 边界框信息会保存在 shape->bounds 中。
 *
 * @param shape 输入的搜索形状，包含中心点坐标和形状参数
 * @return GeoHashRadius 结构，包含编码哈希、相邻网格和覆盖区域
 */
GeoHashRadius geohashCalculateAreasByShapeWGS84(GeoShape *shape) {
    GeoHashRange long_range, lat_range;
    GeoHashRadius radius;
    GeoHashBits hash;
    GeoHashNeighbors neighbors;
    GeoHashArea area;
    double min_lon, max_lon, min_lat, max_lat;
    int steps;

    geohashBoundingBox(shape, shape->bounds);
    min_lon = shape->bounds[0];
    min_lat = shape->bounds[1];
    max_lon = shape->bounds[2];
    max_lat = shape->bounds[3];

    double longitude = shape->xy[0];
    double latitude = shape->xy[1];
    /* radius_meters is calculated differently in different search types:
     * 1) CIRCULAR_TYPE, just use radius.
     * 2) RECTANGLE_TYPE, we use sqrt((width/2)^2 + (height/2)^2) to
     * calculate the distance from the center point to the corner */
    double radius_meters = shape->type == CIRCULAR_TYPE ? shape->t.radius :
            sqrt((shape->t.r.width/2)*(shape->t.r.width/2) + (shape->t.r.height/2)*(shape->t.r.height/2));
    radius_meters *= shape->conversion;

    steps = geohashEstimateStepsByRadius(radius_meters,latitude);

    geohashGetCoordRange(&long_range,&lat_range);
    geohashEncode(&long_range,&lat_range,longitude,latitude,steps,&hash);
    geohashNeighbors(&hash,&neighbors);
    geohashDecode(long_range,lat_range,hash,&area);

    /* Check if the step is enough at the limits of the covered area.
     * Sometimes when the search area is near an edge of the
     * area, the estimated step is not small enough, since one of the
     * north / south / west / east square is too near to the search area
     * to cover everything. */
    int decrease_step = 0;
    {
        GeoHashArea north, south, east, west;

        geohashDecode(long_range, lat_range, neighbors.north, &north);
        geohashDecode(long_range, lat_range, neighbors.south, &south);
        geohashDecode(long_range, lat_range, neighbors.east, &east);
        geohashDecode(long_range, lat_range, neighbors.west, &west);

        if (north.latitude.max < max_lat) 
            decrease_step = 1;
        if (south.latitude.min > min_lat) 
            decrease_step = 1;
        if (east.longitude.max < max_lon) 
            decrease_step = 1;
        if (west.longitude.min > min_lon)  
            decrease_step = 1;
    }

    if (steps > 1 && decrease_step) {
        steps--;
        geohashEncode(&long_range,&lat_range,longitude,latitude,steps,&hash);
        geohashNeighbors(&hash,&neighbors);
        geohashDecode(long_range,lat_range,hash,&area);
    }

    /* Exclude the search areas that are useless. */
    if (steps >= 2) {
        if (area.latitude.min < min_lat) {
            GZERO(neighbors.south);
            GZERO(neighbors.south_west);
            GZERO(neighbors.south_east);
        }
        if (area.latitude.max > max_lat) {
            GZERO(neighbors.north);
            GZERO(neighbors.north_east);
            GZERO(neighbors.north_west);
        }
        if (area.longitude.min < min_lon) {
            GZERO(neighbors.west);
            GZERO(neighbors.south_west);
            GZERO(neighbors.north_west);
        }
        if (area.longitude.max > max_lon) {
            GZERO(neighbors.east);
            GZERO(neighbors.south_east);
            GZERO(neighbors.north_east);
        }
    }
    radius.hash = hash;
    radius.neighbors = neighbors;
    radius.area = area;
    return radius;
}

/* 将 Geohash 转换为 52 位定点数
 *
 * 用于与 Redis 内部存储格式兼容，将 Geohash 位值左移
 * 到 52 位位置，以适配 double 精度的尾数部分。
 *
 * @param hash 输入的 Geohash 值
 * @return 52 位定点数表示
 */
GeoHashFix52Bits geohashAlign52Bits(const GeoHashBits hash) {
    uint64_t bits = hash.bits;
    bits <<= (52 - hash.step * 2);
    return bits;
}

/* 使用简化的 haversine 大圆距离公式计算纬度距离
 *
 * 当两点经度相同时，可以简化 haversine 公式。
 * 由于纬度范围在 [-π/2, π/2] 之间，asin(sin(x)) = x。
 *
 * @param lat1d 第一个点的纬度（度）
 * @param lat2d 第二个点的纬度（度）
 * @return 两点之间的距离（米）
 */
double geohashGetLatDistance(double lat1d, double lat2d) {
    return EARTH_RADIUS_IN_METERS * fabs(deg_rad(lat2d) - deg_rad(lat1d));
}

/* 使用 haversine 大圆距离公式计算两点之间的距离
 *
 * Haversine 公式用于计算球面上两点之间的最短距离。
 * 适用于地球表面的近距离距离计算。
 *
 * @param lon1d 第一个点的经度（度）
 * @param lat1d 第一个点的纬度（度）
 * @param lon2d 第二个点的经度（度）
 * @param lat2d 第二个点的纬度（度）
 * @return 两点之间的距离（米）
 */
double geohashGetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
    double lat1r, lon1r, lat2r, lon2r, u, v, a;
    lon1r = deg_rad(lon1d);
    lon2r = deg_rad(lon2d);
    v = sin((lon2r - lon1r) / 2);
    /* if v == 0 we can avoid doing expensive math when lons are practically the same */
    if (v == 0.0)
        return geohashGetLatDistance(lat1d, lat2d);
    lat1r = deg_rad(lat1d);
    lat2r = deg_rad(lat2d);
    u = sin((lat2r - lat1r) / 2);
    a = u * u + cos(lat1r) * cos(lat2r) * v * v;
    return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(a));
}

/* 检查点是否在指定半径范围内，若是则计算距离
 *
 * @param x1 第一个点的经度
 * @param y1 第一个点的纬度
 * @param x2 第二个点的经度
 * @param y2 第二个点的纬度
 * @param radius 指定半径
 * @param distance 输出参数，计算得到的距离
 * @return 点在半径内返回 1，否则返回 0
 */
int geohashGetDistanceIfInRadius(double x1, double y1,
                                 double x2, double y2, double radius,
                                 double *distance) {
    *distance = geohashGetDistance(x1, y1, x2, y2);
    if (*distance > radius) return 0;
    return 1;
}

/* 使用 WGS84 坐标系检查点是否在指定半径范围内
 *
 * @param x1 第一个点的经度
 * @param y1 第一个点的纬度
 * @param x2 第二个点的经度
 * @param y2 第二个点的纬度
 * @param radius 指定半径
 * @param distance 输出参数，计算得到的距离
 * @return 点在半径内返回 1，否则返回 0
 */
int geohashGetDistanceIfInRadiusWGS84(double x1, double y1, double x2,
                                      double y2, double radius,
                                      double *distance) {
    return geohashGetDistanceIfInRadius(x1, y1, x2, y2, radius, distance);
}

/* 判断点是否在轴对齐的矩形内
 *
 * 当搜索点与中心点的纬度距离小于等于 height/2，
 * 经度距离小于等于 width/2 时，点在矩形内。
 *
 * @param width_m  矩形宽度（米）
 * @param height_m 矩形高度（米）
 * @param x1       矩形中心经度
 * @param y1       矩形中心纬度
 * @param x2       待检测点经度
 * @param y2       待检测点纬度
 * @param distance 输出参数，两点间实际距离
 * @return 点在矩形内返回 1，否则返回 0
 */
int geohashGetDistanceIfInRectangle(double width_m, double height_m, double x1, double y1,
                                    double x2, double y2, double *distance) {
    /* latitude distance is less expensive to compute than longitude distance
     * so we check first for the latitude condition */
    double lat_distance = geohashGetLatDistance(y2, y1);
    if (lat_distance > height_m/2) {
        return 0;
    }
    double lon_distance = geohashGetDistance(x2, y2, x1, y2);
    if (lon_distance > width_m/2) {
        return 0;
    }
    *distance = geohashGetDistance(x1, y1, x2, y2);
    return 1;
}
