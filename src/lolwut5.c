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

/* 将一组 8 个像素（2x4 的竖直矩形）翻译为对应的盲文字符。
 * 该字节应当按照如下方式对应于这些像素，其中 0 是最低有效位，
 * 7 是最高有效位：
 *
 *   0 3
 *   1 4
 *   2 5
 *   6 7
 *
 * 对应的 utf8 编码字符被设置到 'output' 指向的三个字节中。
 */
#include <stdio.h>
void lwTranslatePixelsGroup(int byte, char *output) {
    int code = 0x2800 + byte;
    /* 转换为 Unicode。该码点位于 U0800-UFFFF 区间，因此我们
     * 需要按以下三字节方式输出：
     * 1110xxxx 10xxxxxx 10xxxxxx。 */
    output[0] = 0xE0 | (code >> 12);          /* 1110-xxxx */
    output[1] = 0x80 | ((code >> 6) & 0x3F);  /* 10-xxxxxx */
    output[2] = 0x80 | (code & 0x3F);         /* 10-xxxxxx */
}

/* Schotter 是 Redis 5 中 LOLWUT 命令的输出，它是一件由
 * Georg Nees 于 60 年代创作的计算机图形艺术作品，探讨了
 * 混沌与秩序之间的关系。
 *
 * 该函数根据输出显示中可用的列数以及调用者请求的每行
 * 和每列的方块数量来创建画布本身。 */
lwCanvas *lwDrawSchotter(int console_cols, int squares_per_row, int squares_per_col) {
    /* 计算画布尺寸。 */
    int canvas_width = console_cols*2;
    int padding = canvas_width > 4 ? 2 : 0;
    float square_side = (float)(canvas_width-padding*2) / squares_per_row;
    int canvas_height = square_side * squares_per_col + padding*2;
    lwCanvas *canvas = lwCreateCanvas(canvas_width, canvas_height, 0);

    for (int y = 0; y < squares_per_col; y++) {
        for (int x = 0; x < squares_per_row; x++) {
            int sx = x * square_side + square_side/2 + padding;
            int sy = y * square_side + square_side/2 + padding;
            /* 当我们往下走到较低的行时，随机地进行旋转和平移。 */
            float angle = 0;
            if (y > 1) {
                float r1 = (float)rand() / (float) RAND_MAX / squares_per_col * y;
                float r2 = (float)rand() / (float) RAND_MAX / squares_per_col * y;
                float r3 = (float)rand() / (float) RAND_MAX / squares_per_col * y;
                if (rand() % 2) r1 = -r1;
                if (rand() % 2) r2 = -r2;
                if (rand() % 2) r3 = -r3;
                angle = r1;
                sx += r2*square_side/3;
                sy += r3*square_side/3;
            }
            lwDrawSquare(canvas,sx,sy,square_side,angle,1);
        }
    }

    return canvas;
}

/* 将画布转换为 SDS 字符串，该字符串表示需要打印到终端的 UTF8 字符，
 * 以便获得逻辑画布的图形化表示。由于每个 Braille 字符占 2x4，
 * 所以实际返回的字符串需要一个宽度为 width/2、高度为 height/4 的
 * 终端才能容纳整个图像而不会溢出或滚动。 */
static sds renderCanvas(lwCanvas *canvas) {
    sds text = sdsempty();
    for (int y = 0; y < canvas->height; y += 4) {
        for (int x = 0; x < canvas->width; x += 2) {
            /* 我们需要按照特定排列输出 8 位一组的数据。
             * 详见 lwTranslatePixelsGroup()。 */
            int byte = 0;
            if (lwGetPixel(canvas,x,y)) byte |= (1<<0);
            if (lwGetPixel(canvas,x,y+1)) byte |= (1<<1);
            if (lwGetPixel(canvas,x,y+2)) byte |= (1<<2);
            if (lwGetPixel(canvas,x+1,y)) byte |= (1<<3);
            if (lwGetPixel(canvas,x+1,y+1)) byte |= (1<<4);
            if (lwGetPixel(canvas,x+1,y+2)) byte |= (1<<5);
            if (lwGetPixel(canvas,x,y+3)) byte |= (1<<6);
            if (lwGetPixel(canvas,x+1,y+3)) byte |= (1<<7);
            char unicode[3];
            lwTranslatePixelsGroup(byte,unicode);
            text = sdscatlen(text,unicode,3);
        }
        if (y != canvas->height-1) text = sdscatlen(text,"\n",1);
    }
    return text;
}

/* LOLWUT 命令：
 *
 * LOLWUT [terminal columns] [squares-per-row] [squares-per-col]
 *
 * 默认情况下，该命令使用 66 列、每行 8 个方块、每列 12 个方块。
 */
void lolwut5Command(client *c) {
    long cols = 66;
    long squares_per_row = 8;
    long squares_per_col = 12;

    /* 解析可选参数（如果有）。 */
    if (c->argc > 1 &&
        getLongFromObjectOrReply(c,c->argv[1],&cols,NULL) != C_OK)
        return;

    if (c->argc > 2 &&
        getLongFromObjectOrReply(c,c->argv[2],&squares_per_row,NULL) != C_OK)
        return;

    if (c->argc > 3 &&
        getLongFromObjectOrReply(c,c->argv[3],&squares_per_col,NULL) != C_OK)
        return;

    /* 限制范围。我们希望 LOLWUT 始终保持合理快速的执行，
     * 因此对列数、行数和输出分辨率都设置了最大值。 */
    if (cols < 1) cols = 1;
    if (cols > 1000) cols = 1000;
    if (squares_per_row < 1) squares_per_row = 1;
    if (squares_per_row > 200) squares_per_row = 200;
    if (squares_per_col < 1) squares_per_col = 1;
    if (squares_per_col > 200) squares_per_col = 200;

    /* 生成一些计算机艺术图并回复给客户端。 */
    lwCanvas *canvas = lwDrawSchotter(cols,squares_per_row,squares_per_col);
    sds rendered = renderCanvas(canvas);
    rendered = sdscat(rendered,
        "\nGeorg Nees - schotter, plotter on paper, 1968. Redis ver. ");
    rendered = sdscat(rendered,REDIS_VERSION);
    rendered = sdscatlen(rendered,"\n",1);
    addReplyVerbatim(c,rendered,sdslen(rendered),"txt");
    sdsfree(rendered);
    lwFreeCanvas(canvas);
}
