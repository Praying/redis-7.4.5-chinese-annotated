/*
 * Copyright (c) 2018-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * ----------------------------------------------------------------------------
 *
 * 本文件实现 LOLWUT 命令。该命令应当执行一些有趣的事情，
 * 并且应该在 Redis 的每个新版本中替换为新的实现。
 */

#include "server.h"
#include "lolwut.h"
#include <math.h>

void lolwut5Command(client *c);
void lolwut6Command(client *c);

/* 当 LOLWUT 未找到匹配的版本时使用的默认实现。
 * 不稳定版本的 Redis 将会显示该输出。 */
void lolwutUnstableCommand(client *c) {
    sds rendered = sdsnew("Redis ver. ");
    rendered = sdscat(rendered,REDIS_VERSION);
    rendered = sdscatlen(rendered,"\n",1);
    addReplyVerbatim(c,rendered,sdslen(rendered),"txt");
    sdsfree(rendered);
}

/* LOLWUT [VERSION <version>] [... 版本相关的参数 ...] */
void lolwutCommand(client *c) {
    char *v = REDIS_VERSION;
    char verstr[64];

    if (c->argc >= 3 && !strcasecmp(c->argv[1]->ptr,"version")) {
        long ver;
        if (getLongFromObjectOrReply(c,c->argv[2],&ver,NULL) != C_OK) return;
        snprintf(verstr,sizeof(verstr),"%u.0.0",(unsigned int)ver);
        v = verstr;

        /* 调整 argv/argc 以过滤掉 "VERSION ..." 选项，因为具体版本的
         * LOLWUT 实现并不知道该选项，并期望接收它们自己的参数。 */
        c->argv += 2;
        c->argc -= 2;
    }

    /* 根据版本号分发到对应的 LOLWUT 实现 */
    if ((v[0] == '5' && v[1] == '.' && v[2] != '9') ||
        (v[0] == '4' && v[1] == '.' && v[2] == '9'))
        lolwut5Command(c);
    else if ((v[0] == '6' && v[1] == '.' && v[2] != '9') ||
             (v[0] == '5' && v[1] == '.' && v[2] == '9'))
        lolwut6Command(c);
    else
        lolwutUnstableCommand(c);

    /* 如果使用了 VERSION 参数，则恢复 argc/argv。 */
    if (v == verstr) {
        c->argv -= 2;
        c->argc += 2;
    }
}

/* ========================== LOLWUT 画布 =================================
 * 许多 LOLWUT 版本可能会在屏幕上打印一些计算机艺术图。
 * LOLWUT 5 和 LOLWUT 6 就是这种情况，因此这里有一个通用的
 * 画布实现，可以被复用。  */

/* 分配并返回一个具有指定尺寸的新画布。 */
lwCanvas *lwCreateCanvas(int width, int height, int bgcolor) {
    lwCanvas *canvas = zmalloc(sizeof(*canvas));
    canvas->width = width;
    canvas->height = height;
    canvas->pixels = zmalloc((size_t)width*height);
    memset(canvas->pixels,bgcolor,(size_t)width*height);
    return canvas;
}

/* 释放由 lwCreateCanvas() 创建的画布。 */
void lwFreeCanvas(lwCanvas *canvas) {
    zfree(canvas->pixels);
    zfree(canvas);
}

/* 将一个像素设置为指定的颜色。颜色为 0 或 1，其中 0 表示不显示点，
 * 1 表示显示点。坐标系以左上角为 (0,0)。即使写入到画布范围之外
 * 也不会产生问题。 */
void lwDrawPixel(lwCanvas *canvas, int x, int y, int color) {
    if (x < 0 || x >= canvas->width ||
        y < 0 || y >= canvas->height) return;
    canvas->pixels[x+y*canvas->width] = color;
}

/* 返回画布上指定像素的值。 */
int lwGetPixel(lwCanvas *canvas, int x, int y) {
    if (x < 0 || x >= canvas->width ||
        y < 0 || y >= canvas->height) return 0;
    return canvas->pixels[x+y*canvas->width];
}

/* 使用 Bresenham 算法从 (x1,y1) 到 (x2,y2) 画一条线。 */
void lwDrawLine(lwCanvas *canvas, int x1, int y1, int x2, int y2, int color) {
    int dx = abs(x2-x1);
    int dy = abs(y2-y1);
    int sx = (x1 < x2) ? 1 : -1;
    int sy = (y1 < y2) ? 1 : -1;
    int err = dx-dy, e2;

    while(1) {
        lwDrawPixel(canvas,x1,y1,color);
        if (x1 == x2 && y1 == y2) break;
        e2 = err*2;
        if (e2 > -dy) {
            err -= dy;
            x1 += sx;
        }
        if (e2 < dx) {
            err += dx;
            y1 += sy;
        }
    }
}

/* 在指定的 x,y 坐标处绘制一个具有给定旋转角度和大小的正方形。
 * 为了绘制旋转的正方形，我们使用以下平凡事实 —— 参数方程：
 *
 *  x = sin(k)
 *  y = cos(k)
 *
 * 当 k 从 0 取到 2*PI 时，描述的是一个圆。因此如果我们从 45 度
 * 也就是 k = PI/4 开始取第一个点，然后将 k 每次增加 PI/2（90 度）
 * 得到其余三个点，就得到正方形的四个顶点。为了旋转正方形，
 * 我们只需将初始的 k 设为 PI/4 + rotation_angle 即可。
 *
 * 当然上面的原始方程所描述的正方形是内嵌在半径为 1 的圆中的，
 * 因此为了绘制更大的正方形，我们需要将得到的坐标乘以缩放因子，
 * 然后再平移。然而这比实现 2D 形状的抽象概念并执行旋转/平移变换
 * 要简单得多，所以对于 LOLWUT 来说这是一个很好的方法。 */
void lwDrawSquare(lwCanvas *canvas, int x, int y, float size, float angle, int color) {
    int px[4], py[4];

    /* 根据内嵌于半径为 1 的圆中的正方形的边长为 SQRT(2) 这一事实，
     * 对期望的尺寸进行调整。这样 size 就变成一个简单的乘数因子，
     * 我们可以用它来放大坐标。 */
    size /= 1.4142135623;
    size = round(size);

    /* 计算四个顶点。 */
    float k = M_PI/4 + angle;
    for (int j = 0; j < 4; j++) {
        px[j] = round(sin(k) * size + x);
        py[j] = round(cos(k) * size + y);
        k += M_PI/2;
    }

    /* 绘制正方形。 */
    for (int j = 0; j < 4; j++)
        lwDrawLine(canvas,px[j],py[j],px[(j+1)%4],py[(j+1)%4],color);
}
