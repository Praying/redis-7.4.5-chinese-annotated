/* sparkline.c -- ASCII 迷你图生成器
 *
 * 本代码修改自 http://github.com/antirez/aspark,
 * 适配为返回 SDS 字符串而不是直接输出到终端.
 *
 * 迷你图(Sparkline)是一种极简的、数据密集的、字符合成的图表,
 * 适合在命令行或纯文本环境中展示数据趋势.
 *
 * ---------------------------------------------------------------------------
 *
 * Copyright (c) 2011-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"

#include <math.h>

/* 迷你图的字符集定义
 *
 * charset:     普通模式使用的字符,按从低到高的顺序排列
 * charset_fill: 填充模式使用的字符,按从低到高的顺序排列
 *
 * 字符集对应关系:
 *   低值 -> 高值
 *   普通: _  -  `  (3级高度)
 *   填充: _  o  #  (3级高度,填充模式用不同字符)
 *
 * 多个行叠加使用以增加垂直分辨率.
 */
static char charset[] = "_-`";
static char charset_fill[] = "_o#";
static int charset_len = sizeof(charset)-1;
static int label_margin_top = 1; /* 标签与图表之间的行间距 */

/* ----------------------------------------------------------------------------
 * Sparkline API 使用示例:
 *
 * // 创建序列
 * struct sequence *seq = createSparklineSequence();
 *
 * // 添加数据样本
 * sparklineSequenceAddSample(seq, 10, NULL);           // 无标签
 * sparklineSequenceAddSample(seq, 20, NULL);           // 无标签
 * sparklineSequenceAddSample(seq, 30, "last sample label"); // 带标签
 *
 * // 渲染迷你图(80列宽, 4行高, 填充模式)
 * sds output = sparklineRender(sdsempty(), seq, 80, 4, SPARKLINE_FILL);
 *
 * // 释放资源
 * freeSparklineSequence(seq);
 * ------------------------------------------------------------------------- */

/* Create a new sequence. */
struct sequence *createSparklineSequence(void) {
    struct sequence *seq = zmalloc(sizeof(*seq));
    seq->length = 0;
    seq->labels = 0;
    seq->samples = NULL;
    seq->min = 0.0f;
    seq->max = 0.0f;
    return seq;
}

/* Add a new sample into a sequence. */
void sparklineSequenceAddSample(struct sequence *seq, double value, char *label) {
    label = (label == NULL || label[0] == '\0') ? NULL : zstrdup(label);
    if (seq->length == 0) {
        seq->min = seq->max = value;
    } else {
        if (value < seq->min) seq->min = value;
        else if (value > seq->max) seq->max = value;
    }
    seq->samples = zrealloc(seq->samples,sizeof(struct sample)*(seq->length+1));
    seq->samples[seq->length].value = value;
    seq->samples[seq->length].label = label;
    seq->length++;
    if (label) seq->labels++;
}

/* Free a sequence. */
void freeSparklineSequence(struct sequence *seq) {
    int j;

    for (j = 0; j < seq->length; j++)
        zfree(seq->samples[j].label);
    zfree(seq->samples);
    zfree(seq);
}

/* ----------------------------------------------------------------------------
 * ASCII rendering of sequence
 * ------------------------------------------------------------------------- */

/* sparklineRenderRange - 渲染序列的一部分
 *
 * 渲染序列的一个子区间,用于生成分段输出以避免超出终端列宽.
 * sparklineRender() 会多次调用此函数,每次处理不同的区间.
 *
 * 参数:
 *   output - 输出的 SDS 字符串(追加模式)
 *   seq    - 数据序列
 *   rows   - 垂直行数(高度)
 *   offset - 序列起始偏移
 *   len    - 要渲染的样本数量
 *   flags  - 渲染标志(SPARKLINE_FILL, SPARKLINE_LOG_SCALE 等)
 *
 * 返回:
 *   追加了渲染结果的新 SDS 字符串
 */
sds sparklineRenderRange(sds output, struct sequence *seq, int rows, int offset, int len, int flags) {
    int j;
    double relmax = seq->max - seq->min;
    int steps = charset_len*rows;
    int row = 0;
    char *chars = zmalloc(len);
    int loop = 1;
    int opt_fill = flags & SPARKLINE_FILL;
    int opt_log = flags & SPARKLINE_LOG_SCALE;

    if (opt_log) {
        relmax = log(relmax+1);
    } else if (relmax == 0) {
        relmax = 1;
    }

    while(loop) {
        loop = 0;
        memset(chars,' ',len);
        for (j = 0; j < len; j++) {
            struct sample *s = &seq->samples[j+offset];
            double relval = s->value - seq->min;
            int step;

            if (opt_log) relval = log(relval+1);
            step = (int) (relval*steps)/relmax;
            if (step < 0) step = 0;
            if (step >= steps) step = steps-1;

            if (row < rows) {
                /* Print the character needed to create the sparkline */
                int charidx = step-((rows-row-1)*charset_len);
                loop = 1;
                if (charidx >= 0 && charidx < charset_len) {
                    chars[j] = opt_fill ? charset_fill[charidx] :
                                          charset[charidx];
                } else if(opt_fill && charidx >= charset_len) {
                    chars[j] = '|';
                }
            } else {
                /* Labels spacing */
                if (seq->labels && row-rows < label_margin_top) {
                    loop = 1;
                    break;
                }
                /* Print the label if needed. */
                if (s->label) {
                    int label_len = strlen(s->label);
                    int label_char = row - rows - label_margin_top;

                    if (label_len > label_char) {
                        loop = 1;
                        chars[j] = s->label[label_char];
                    }
                }
            }
        }
        if (loop) {
            row++;
            output = sdscatlen(output,chars,len);
            output = sdscatlen(output,"\n",1);
        }
    }
    zfree(chars);
    return output;
}

/* sparklineRender - 将数据序列渲染为 ASCII 迷你图
 *
 * 将数据序列转换为 ASCII 字符图形表示.
 * 如果序列长度超过列数,会分多行渲染.
 *
 * 参数:
 *   output  - 输出的 SDS 字符串(追加模式)
 *   seq     - 数据序列
 *   columns - 水平列数(宽度)
 *   rows    - 垂直行数(高度)
 *   flags   - 渲染标志
 *     SPARKLINE_FILL      - 使用填充字符模式
 *     SPARKLINE_LOG_SCALE - 使用对数刻度
 *
 * 返回:
 *   追加了迷你图的新 SDS 字符串
 */
sds sparklineRender(sds output, struct sequence *seq, int columns, int rows, int flags) {
    int j;

    for (j = 0; j < seq->length; j += columns) {
        int sublen = (seq->length-j) < columns ? (seq->length-j) : columns;

        if (j != 0) output = sdscatlen(output,"\n",1);
        output = sparklineRenderRange(output, seq, rows, j, sublen, flags);
    }
    return output;
}

