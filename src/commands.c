/*
 * commands.c - Redis 命令定义的展开入口文件
 *
 * 本文件通过 #include 展开 commands.def（或
 * commands_with_reply_schema.def）中定义的所有 Redis
 * 内置命令。commands.def 中使用 MAKE_CMD / MAKE_ARG 宏
 * 逐条描述每个命令的元数据（名称、摘要、复杂度、参数等），
 * 在此处提供这些宏的实际展开定义，从而在编译期生成全局
 * 命令表（redisCommand 结构体数组）。
 */

#include "commands.h"
#include "server.h"

/* MAKE_CMD 宏：将 commands.def 中的命令描述展开为
 * redisCommand 结构体的初始化字段列表。
 * 去除了 doc_flags 和 group 等用于文档生成的字段，
 * 仅保留运行时所需的实际字段。 */
#define MAKE_CMD(name,summary,complexity,since,doc_flags,replaced,deprecated,group,group_enum,history,num_history,tips,num_tips,function,arity,flags,acl,key_specs,key_specs_num,get_keys,numargs) name,summary,complexity,since,doc_flags,replaced,deprecated,group_enum,history,num_history,tips,num_tips,function,arity,flags,acl,key_specs,key_specs_num,get_keys,numargs

/* MAKE_ARG 宏：将 commands.def 中的命令参数描述展开为
 * redisCommandArg 结构体的初始化字段列表。
 * 调整了字段顺序以匹配结构体的实际内存布局。 */
#define MAKE_ARG(name,type,key_spec_index,token,summary,since,flags,numsubargs,deprecated_since) name,type,key_spec_index,token,summary,since,flags,deprecated_since,numsubargs

/* COMMAND_STRUCT / COMMAND_ARG：在 commands.def 中使用的
 * 类型别名，此处映射到实际的结构体类型名。 */
#define COMMAND_STRUCT redisCommand
#define COMMAND_ARG redisCommandArg

/* LOG_REQ_RES 模式下使用带请求/响应 schema 的命令定义文件，
 * 否则使用标准命令定义文件。 */
#ifdef LOG_REQ_RES
#include "commands_with_reply_schema.def"
#else
#include "commands.def"
#endif
