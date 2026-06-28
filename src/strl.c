/*
 * Copyright (c) 1998, 2015 Todd C. Miller <millert@openbsd.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#include <string.h>

/*
 * 将字符串 src 复制到大小为 dsize 的缓冲区 dst 中。
 * 最多复制 dsize-1 个字符。始终以 NUL 终止（除非 dsize == 0）。
 * 返回 strlen(src)；若返回值 >= dsize，则发生了截断。
 */
size_t
redis_strlcpy(char *dst, const char *src, size_t dsize)
{
    const char *osrc = src;
    size_t nleft = dsize;

    /* 尽可能多地复制字节。 */
    if (nleft != 0) {
        while (--nleft != 0) {
            if ((*dst++ = *src++) == '\0')
                break;
        }
    }

    /* dst 空间不足，添加 NUL 并遍历 src 剩余部分。 */
    if (nleft == 0) {
        if (dsize != 0)
            *dst = '\0';        /* 以 NUL 终止 dst */
        while (*src++)
            ;
    }

    return(src - osrc - 1); /* 计数不包含 NUL */
}

/*
 * 将 src 追加到大小为 dsize 的字符串 dst 末尾
 * （与 strncat 不同，dsize 是 dst 的总大小，而非剩余空间）。
 * 最多复制 dsize-1 个字符。
 * 始终以 NUL 终止（除非 dsize <= strlen(dst)）。
 * 返回 strlen(src) + MIN(dsize, strlen(初始 dst))。
 * 若返回值 >= dsize，则发生了截断。
 */
size_t
redis_strlcat(char *dst, const char *src, size_t dsize)
{
    const char *odst = dst;
    const char *osrc = src;
    size_t n = dsize;
    size_t dlen;

    /* 找到 dst 的末尾，调整剩余字节数，但不超过缓冲区末尾。 */
    while (n-- != 0 && *dst != '\0')
        dst++;
    dlen = dst - odst;          /* dst 当前已用长度 */
    n = dsize - dlen;           /* dst 剩余可用空间 */

    if (n-- == 0)
        return(dlen + strlen(src));
    while (*src != '\0') {
        if (n != 0) {
            *dst++ = *src;
            n--;
        }
        src++;
    }
    *dst = '\0';

    return(dlen + (src - osrc));    /* 计数不包含 NUL */
}





