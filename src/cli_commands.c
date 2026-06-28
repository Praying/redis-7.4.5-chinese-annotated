/* CLI 命令定义文件
 *
 * 本文件通过宏定义将 commands.def 中的命令信息转换为 CLI 命令结构体。
 * 这些宏展开后生成命令名称、摘要、复杂度、版本信息等字段。
 *
 * Copyright (c) 2020-Present, Redis Ltd.
 * All rights reserved.
 */

#include <stddef.h>
#include "cli_commands.h"

/* 用于配置 commands.c 生成结构体的宏定义 */
#define MAKE_CMD(name,summary,complexity,since,doc_flags,replaced,deprecated,group,group_enum,history,num_history,tips,num_tips,function,arity,flags,acl,key_specs,key_specs_num,get_keys,numargs) name,summary,group,since,numargs
/* CLI 命令参数定义宏 */
#define MAKE_ARG(name,type,key_spec_index,token,summary,since,flags,numsubargs,deprecated_since) name,type,token,since,flags,numsubargs
#define COMMAND_ARG cliCommandArg
#define COMMAND_STRUCT commandDocs
#define SKIP_CMD_HISTORY_TABLE    /* 跳过命令历史表 */
#define SKIP_CMD_TIPS_TABLE       /* 跳过命令提示表 */
#define SKIP_CMD_KEY_SPECS_TABLE  /* 跳过键规格表 */

#include "commands.def"
