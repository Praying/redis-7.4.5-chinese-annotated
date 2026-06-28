/* redisassert.c -- Redis 断言和 panic 处理默认实现
 *
 * 本文件实现了默认的 _serverAssert 和 _serverPanic 函数，
 * 用于在断言失败或 panic 时向标准错误流打印堆栈跟踪信息。
 *
 * 被那些需要打印堆栈信息但自己没有实现 redisassert.h 中函数的模块共享使用。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2021, Andy Pan <panjf2000@gmail.com> and Redis Ltd.
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


#include <stdio.h> 
#include <stdlib.h>
#include <signal.h>

/* _serverAssert - 断言失败处理函数
 * estr: 断言表达式字符串
 * file: 断言失败所在的源文件
 * line: 断言失败所在的行号
 * 发送 SIGSEGV 信号以生成核心转储（core dump） */
void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "=== ASSERTION FAILED ===");
    fprintf(stderr, "==> %s:%d '%s' is not true",file,line,estr);
    raise(SIGSEGV);
}

/* _serverPanic - 严重错误（panic）处理函数
 * 打印错误信息后调用 abort() 终止程序 */
void _serverPanic(const char *file, int line, const char *msg, ...) {
    fprintf(stderr, "------------------------------------------------");
    fprintf(stderr, "!!! Software Failure. Press left mouse button to continue");
    fprintf(stderr, "Guru Meditation: %s #%s:%d",msg,file,line);
    abort();
}
