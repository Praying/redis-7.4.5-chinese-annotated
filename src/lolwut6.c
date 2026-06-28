/*
 * Copyright (c) 2019-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * ----------------------------------------------------------------------------
 *
 * 本文件实现 LOLWUT 命令。该命令应当执行一些有趣的事情，
 * 并且应该在 Redis 的每个新版本中替换为新的实现。
 *
 * 感谢 Michele Hiki Falcone 提供了启发本作品的原始图像，
 * 该图像来自他的游戏 Plaguemon。
 *
 * 感谢 Shhh 计算机艺术集体在调优输出以获得更好艺术效果方面
 * 给予的帮助。
 */

#include "server.h"
#include "lolwut.h"

/* 使用标准彩色终端的四个灰度级别来渲染画布：它们与 Game Boy
 * 的灰度显示非常匹配。 */
static sds renderCanvas(lwCanvas *canvas) {
    sds text = sdsempty();
    for (int y = 0; y < canvas->height; y++) {
        for (int x = 0; x < canvas->width; x++) {
            int color = lwGetPixel(canvas,x,y);
            char *ce; /* 颜色转义序列。 */

            /* 注意我们同时设置了前景色和背景色。这样我们能够在
             * 不同的终端实现之间获得更一致的结果。 */
            switch(color) {
            case 0: ce = "0;30;40m"; break;    /* 黑色 */
            case 1: ce = "0;90;100m"; break;   /* 灰色 1 */
            case 2: ce = "0;37;47m"; break;    /* 灰色 2 */
            case 3: ce = "0;97;107m"; break;   /* 白色 */
            default: ce = "0;30;40m"; break;   /* 仅作为安全兜底。 */
            }
            text = sdscatprintf(text,"\033[%s \033[0m",ce);
        }
        if (y != canvas->height-1) text = sdscatlen(text,"\n",1);
    }
    return text;
}

/* 根据 'skyscraper' 结构中的参数在画布上绘制一座摩天大楼。
 * 窗户的颜色是随机的，且始终是两种灰色之一。 */
struct skyscraper {
    int xoff;       /* X 偏移。 */
    int width;      /* 像素宽度。 */
    int height;     /* 像素高度。 */
    int windows;    /* 若为真则绘制窗户。 */
    int color;      /* 摩天大楼的颜色。 */
};

void generateSkyscraper(lwCanvas *canvas, struct skyscraper *si) {
    int starty = canvas->height-1;
    int endy = starty - si->height + 1;
    for (int y = starty; y >= endy; y--) {
        for (int x = si->xoff; x < si->xoff+si->width; x++) {
            /* 屋顶的宽度要少四个像素。 */
            if (y == endy && (x <= si->xoff+1 || x >= si->xoff+si->width-2))
                continue;
            int color = si->color;
            /* 如果我们想在此处绘制窗户，则修改颜色。
             * 我们检查当前位置是否在摩天大楼的内部区域，
             * 以使窗户远离边界。 */
            if (si->windows &&
                x > si->xoff+1 &&
                x < si->xoff+si->width-2 &&
                y > endy+1 &&
                y < starty-1)
            {
                /* 计算相对于窗户区域起点的 x,y 位置。 */
                int relx = x - (si->xoff+1);
                int rely = y - (endy+1);

                /* 注意我们希望窗户的宽度为两个像素但高度仅为一个像素，
                 * 因为终端的“像素”（字符）不是正方形的。 */
                if (relx/2 % 2 && rely % 2) {
                    do {
                        color = 1 + rand() % 2;
                    } while (color == si->color);
                    /* 不过我们希望组成同一扇窗户的相邻像素颜色相同。 */
                    if (relx % 2) color = lwGetPixel(canvas,x-1,y);
                }
            }
            lwDrawPixel(canvas,x,y,color);
        }
    }
}

/* 生成受 8 位游戏视差背景启发而来的城市天际线。 */
void generateSkyline(lwCanvas *canvas) {
    struct skyscraper si;

    /* 首先使用两种不同的灰色绘制背景摩天大楼（不带窗户）。
     * 我们采用两遍绘制以确保较浅的始终位于背景中。 */
    for (int color = 2; color >= 1; color--) {
        si.color = color;
        for (int offset = -10; offset < canvas->width;) {
            offset += rand() % 8;
            si.xoff = offset;
            si.width = 10 + rand()%9;
            if (color == 2)
                si.height = canvas->height/2 + rand()%canvas->height/2;
            else
                si.height = canvas->height/2 + rand()%canvas->height/3;
            si.windows = 0;
            generateSkyscraper(canvas, &si);
            if (color == 2)
                offset += si.width/2;
            else
                offset += si.width+1;
        }
    }

    /* 现在绘制带窗户的前景摩天大楼。 */
    si.color = 0;
    for (int offset = -10; offset < canvas->width;) {
        offset += rand() % 8;
        si.xoff = offset;
        si.width = 5 + rand()%14;
        if (si.width % 4) si.width += (si.width % 3);
        si.height = canvas->height/3 + rand()%canvas->height/2;
        si.windows = 1;
        generateSkyscraper(canvas, &si);
        offset += si.width+5;
    }
}

/* LOLWUT 6 命令：
 *
 * LOLWUT [columns] [rows]
 *
 * 默认情况下，该命令使用 80 列、每列 40 个方块。
 */
void lolwut6Command(client *c) {
    long cols = 80;
    long rows = 20;

    /* 解析可选参数（如果有）。 */
    if (c->argc > 1 &&
        getLongFromObjectOrReply(c,c->argv[1],&cols,NULL) != C_OK)
        return;

    if (c->argc > 2 &&
        getLongFromObjectOrReply(c,c->argv[2],&rows,NULL) != C_OK)
        return;

    /* 限制范围。我们希望 LOLWUT 始终保持合理快速的执行，
     * 因此对列数、行数和输出分辨率都设置了最大值。 */
    if (cols < 1) cols = 1;
    if (cols > 1000) cols = 1000;
    if (rows < 1) rows = 1;
    if (rows > 1000) rows = 1000;

    /* 生成城市天际线并回复给客户端。 */
    lwCanvas *canvas = lwCreateCanvas(cols,rows,3);
    generateSkyline(canvas);
    sds rendered = renderCanvas(canvas);
    rendered = sdscat(rendered,
        "\nDedicated to the 8 bit game developers of past and present.\n"
        "Original 8 bit image from Plaguemon by hikikomori. Redis ver. ");
    rendered = sdscat(rendered,REDIS_VERSION);
    rendered = sdscatlen(rendered,"\n",1);
    addReplyVerbatim(c,rendered,sdslen(rendered),"txt");
    sdsfree(rendered);
    lwFreeCanvas(canvas);
}
