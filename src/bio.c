/* Redis 后台 I/O 服务。
 *
 * 本文件实现了需要在后台执行的操作。当前共有 3 类操作：
 * 1) 后台 close(2) 系统调用。当进程是文件引用的最后一个拥有者时，
 *    关闭该文件意味着要 unlink 它，而删除文件可能很慢，会阻塞服务器。
 * 2) AOF fsync（AOF 文件同步）
 * 3) lazyfree（惰性释放内存）
 *
 * 未来我们可能会继续实现新的后台任务，或者切换到 libeio。
 * 不过这个文件长期来看仍然有用，因为我们可以把 Redis 特有的后台任务
 * 放在这里。
 *
 * 设计说明
 * ------
 *
 * 设计很简单：我们用一个结构体表示一个待执行的作业，
 * 并配有若干工作线程和作业队列。每种作业类型被分配到
 * 一个特定的工作线程，一个工作线程可以处理多种不同的作业类型。
 * 每个线程在自己的队列上等待新作业，并按顺序处理每个作业。
 *
 * 由同一个工作线程处理的作业保证按照从最早插入到最近插入的
 * 顺序进行处理（较早的作业先处理）。
 *
 * 为了让作业创建者能够在操作完成时收到通知，它需要额外提交一个
 * 虚拟作业（称为“完成作业请求”），后台线程最终会将其回写到
 * 完成作业响应队列中。这种通知机制可以简化需要提交多个作业的流程，
 * 例如 FLUSHALL 会为单个命令提交多个作业。由于作业按 FIFO 顺序处理，
 * 因此这种机制也是正确的。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "bio.h"
#include <fcntl.h>

/* 后台工作线程的名称数组。下标对应 bio_worker_t 枚举值。
 * 用作 pthread 的 thread title，方便调试时识别线程类型。 */
static char* bio_worker_title[] = {
    "bio_close_file",   // 负责异步关闭文件的工作线程
    "bio_aof",          // 负责 AOF fsync / 关闭 AOF 的工作线程
    "bio_lazy_free",    // 负责惰性释放内存的工作线程
};

/* 后台工作线程数量：由线程名称数组长度推导而来。 */
#define BIO_WORKER_NUM (sizeof(bio_worker_title) / sizeof(*bio_worker_title))

/* 作业类型到工作线程下标的映射表。
 * 用于在 bioSubmitJob 中根据作业类型找到对应的后台线程。 */
static unsigned int bio_job_to_worker[] = {
    [BIO_CLOSE_FILE] = 0,            // 关闭文件 -> 工作线程 0
    [BIO_AOF_FSYNC] = 1,             // AOF fsync -> 工作线程 1
    [BIO_CLOSE_AOF] = 1,             // 关闭 AOF -> 工作线程 1
    [BIO_LAZY_FREE] = 2,             // 惰性释放 -> 工作线程 2
    [BIO_COMP_RQ_CLOSE_FILE] = 0,    // 关闭文件完成回调 -> 工作线程 0
    [BIO_COMP_RQ_AOF_FSYNC]  = 1,    // AOF fsync 完成回调 -> 工作线程 1
    [BIO_COMP_RQ_LAZY_FREE]  = 2     // 惰性释放完成回调 -> 工作线程 2
};

/* 各工作线程的 pthread 句柄、互斥锁、条件变量、作业队列。
 * BIO_WORKER_NUM 与 bio_worker_title 数组长度一致。 */
static pthread_t bio_threads[BIO_WORKER_NUM];
static pthread_mutex_t bio_mutex[BIO_WORKER_NUM];
static pthread_cond_t bio_newjob_cond[BIO_WORKER_NUM];
static list *bio_jobs[BIO_WORKER_NUM];
/* 每种作业类型的待处理计数。bio_pending_jobs_of_type 用于向 INFO 等接口暴露。 */
static unsigned long bio_jobs_counter[BIO_NUM_OPS] = {0};

/* bio_comp_list 用于保存完成作业响应，并交接给主线程以回调的方式
 * 通知作业完成。主线程通过向管道写入信号来触发读取该列表。 */
static list *bio_comp_list;
static pthread_mutex_t bio_mutex_comp;
static int job_comp_pipe[2];   /* 用于唤醒事件循环的管道 */

typedef struct bio_comp_item {
    comp_fn *func;    /* 作业完成后的回调函数 */
    uint64_t arg;     /* 传递给回调函数的用户数据 */
} bio_comp_item;

/* 该结构体表示一个后台作业。它仅在本文件内使用，
 * 因为 API 完全不向外暴露其内部细节。 */
typedef union bio_job {
    struct {
        int type; /* 作业类型标签。必须是所有联合成员的第一个元素。 */
    } header;

    /* 作业特定的参数。*/
    struct {
        int type;
        int fd; /* 基于文件的后台作业所使用的 fd */
        long long offset; /* 作业相关的偏移量（如适用） */
        unsigned need_fsync:1; /* 指示关闭文件前是否需要执行 fsync。*/
        unsigned need_reclaim_cache:1; /* 指示关闭文件前是否需要回收页缓存。*/
    } fd_args;

    struct {
        int type;
        lazy_free_fn *free_fn; /* 用于释放传入参数的函数 */
        void *free_args[]; /* 传递给释放函数的参数列表（柔性数组） */
    } free_args;
    struct {
        int type; /* 头部 */
        comp_fn *fn; /* 回调函数。交给主线程回调，作为作业完成的通知 */
        uint64_t arg; /* 回调参数 */
    } comp_rq;
} bio_job;

void *bioProcessBackgroundJobs(void *arg);
void bioPipeReadJobCompList(aeEventLoop *el, int fd, void *privdata, int mask);

/* 确保我们有足够的栈空间来执行所有在主线程中做的事情。 */
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* 初始化后台 I/O 系统，派生工作线程。
 * 创建每个工作线程的互斥锁、条件变量、作业队列，完成列表与管道，
 * 并向事件循环注册管道读事件用于接收作业完成通知。 */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    unsigned long j;

    /* 初始化状态变量与对象 */
    for (j = 0; j < BIO_WORKER_NUM; j++) {
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_newjob_cond[j],NULL);
        bio_jobs[j] = listCreate();
    }

    /* 初始化作业完成响应列表 */
    bio_comp_list = listCreate();
    pthread_mutex_init(&bio_mutex_comp, NULL);

    /* 创建一个管道，以便后台线程能够唤醒 Redis 主线程。
     * 将管道设置为非阻塞。这只是一种尽力而为的唤醒机制，
     * 我们不希望在读或写端发生阻塞。
     * 在管道上启用 close-on-exec 标志，以应对 sentinel 或
     * redis 服务器中可能的 fork-exec 系统调用。 */
    if (anetPipe(job_comp_pipe, O_CLOEXEC|O_NONBLOCK, O_CLOEXEC|O_NONBLOCK) == -1) {
        serverLog(LL_WARNING,
                  "Can't create the pipe for bio thread: %s", strerror(errno));
        exit(1);
    }

    /* 为管道注册一个可读事件，用于在作业完成时唤醒事件循环 */
    if (aeCreateFileEvent(server.el, job_comp_pipe[0], AE_READABLE,
                          bioPipeReadJobCompList, NULL) == AE_ERR) {
        serverPanic("Error registering the readable event for the bio pipe.");
    }

    /* 设置栈大小，因为某些系统的默认栈大小可能较小 */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr,&stacksize);
    if (!stacksize) stacksize = 1; /* 世界充满了针对 Solaris 的补丁 */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* 准备派生线程。我们使用线程函数接受的单个参数
     * 来传入该线程所负责的工作线程下标。 */
    for (j = 0; j < BIO_WORKER_NUM; j++) {
        void *arg = (void*)(unsigned long) j;
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            serverLog(LL_WARNING, "Fatal: Can't initialize Background Jobs. Error message: %s", strerror(errno));
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

/* 提交一个后台作业。
 * type 为作业类型（CLOSE_FILE / AOF_FSYNC / LAZY_FREE 等），
 * job 为已分配好的 bio_job 联合体（其 header.type 字段将被覆盖）。
 * 该函数将作业追加到对应工作线程的队尾，并递增计数器与发送条件信号。 */
void bioSubmitJob(int type, bio_job *job) {
    job->header.type = type;
    unsigned long worker = bio_job_to_worker[type];
    pthread_mutex_lock(&bio_mutex[worker]);
    listAddNodeTail(bio_jobs[worker],job);
    bio_jobs_counter[type]++;
    pthread_cond_signal(&bio_newjob_cond[worker]);
    pthread_mutex_unlock(&bio_mutex[worker]);
}

/* 创建并提交一个惰性释放作业。
 * free_fn 为释放函数，arg_count 为可变参数个数。
 * 后台线程会调用 free_fn(free_args[0..arg_count-1])。 */
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...) {
    va_list valist;
    /* 为作业结构体以及所有必需的参数分配内存 */
    bio_job *job = zmalloc(sizeof(*job) + sizeof(void *) * (arg_count));
    job->free_args.free_fn = free_fn;

    va_start(valist, arg_count);
    for (int i = 0; i < arg_count; i++) {
        job->free_args.free_args[i] = va_arg(valist, void *);
    }
    va_end(valist);
    bioSubmitJob(BIO_LAZY_FREE, job);
}

/* 在指定工作线程上注册一个“作业完成”回调请求。
 * 后台线程在完成前置作业后，会将回调写回主线程的完成列表，
 * 主线程随后会在事件循环中执行 func(user_data)。 */
void bioCreateCompRq(bio_worker_t assigned_worker, comp_fn *func, uint64_t user_data) {
    int type;
    switch (assigned_worker) {
        case BIO_WORKER_CLOSE_FILE:
            type = BIO_COMP_RQ_CLOSE_FILE;
            break;
        case BIO_WORKER_AOF_FSYNC:
            type = BIO_COMP_RQ_AOF_FSYNC;
            break;
        case BIO_WORKER_LAZY_FREE:
            type = BIO_COMP_RQ_LAZY_FREE;
            break;
        default:
            serverPanic("Invalid worker type in bioCreateCompRq().");
    }

    bio_job *job = zmalloc(sizeof(*job));
    job->comp_rq.fn = func;
    job->comp_rq.arg = user_data;
    bioSubmitJob(type, job);
}

/* 创建一个“后台关闭文件”作业。
 * fd 为待关闭的文件描述符；need_fsync 表示关闭前是否需要 fsync；
 * need_reclaim_cache 表示关闭前是否需要回收页缓存。 */
void bioCreateCloseJob(int fd, int need_fsync, int need_reclaim_cache) {
    bio_job *job = zmalloc(sizeof(*job));
    job->fd_args.fd = fd;
    job->fd_args.need_fsync = need_fsync;
    job->fd_args.need_reclaim_cache = need_reclaim_cache;

    bioSubmitJob(BIO_CLOSE_FILE, job);
}

/* 创建一个“后台关闭 AOF 文件”作业。
 * fd 为待关闭的 AOF 文件描述符；offset 为对应的复制偏移；
 * need_reclaim_cache 表示关闭前是否需要回收页缓存。 */
void bioCreateCloseAofJob(int fd, long long offset, int need_reclaim_cache) {
    bio_job *job = zmalloc(sizeof(*job));
    job->fd_args.fd = fd;
    job->fd_args.offset = offset;
    job->fd_args.need_fsync = 1;
    job->fd_args.need_reclaim_cache = need_reclaim_cache;

    bioSubmitJob(BIO_CLOSE_AOF, job);
}

/* 创建一个“后台 fsync AOF 文件”作业。
 * fd 为 AOF 文件描述符；offset 为对应的复制偏移；
 * need_reclaim_cache 表示 fsync 后是否需要回收页缓存。 */
void bioCreateFsyncJob(int fd, long long offset, int need_reclaim_cache) {
    bio_job *job = zmalloc(sizeof(*job));
    job->fd_args.fd = fd;
    job->fd_args.offset = offset;
    job->fd_args.need_reclaim_cache = need_reclaim_cache;

    bioSubmitJob(BIO_AOF_FSYNC, job);
}

/* 后台 I/O 线程入口。
 * 每个工作线程负责一个 bio_jobs[worker] 队列，
 * 循环从队列中取出作业并按类型分派执行。
 * 线程退出前会一直循环。 */
void *bioProcessBackgroundJobs(void *arg) {
    bio_job *job;
    unsigned long worker = (unsigned long) arg;
    sigset_t sigset;

    /* 校验工作线程下标在合法范围内。 */
    serverAssert(worker < BIO_WORKER_NUM);

    redis_set_thread_title(bio_worker_title[worker]);

    redisSetCpuAffinity(server.bio_cpulist);

    makeThreadKillable();

    pthread_mutex_lock(&bio_mutex[worker]);
    /* 屏蔽 SIGALRM，确保只有主线程会接收看门狗信号。 */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    while(1) {
        listNode *ln;

        /* 循环每次进入时都持有对应工作线程的锁。 */
        if (listLength(bio_jobs[worker]) == 0) {
            /* 队列为空时阻塞等待新作业信号。 */
            pthread_cond_wait(&bio_newjob_cond[worker], &bio_mutex[worker]);
            continue;
        }
        /* 从队列头部取出一个作业（FIFO）。 */
        ln = listFirst(bio_jobs[worker]);
        job = ln->value;
        /* 由于已经取得了独立的 job 结构体，可以释放后台系统的锁。 */
        pthread_mutex_unlock(&bio_mutex[worker]);

        /* 根据作业类型分派处理逻辑。 */
        int job_type = job->header.type;

        if (job_type == BIO_CLOSE_FILE) {
            // 异步关闭文件：可选 fsync 与页缓存回收
            if (job->fd_args.need_fsync &&
                redis_fsync(job->fd_args.fd) == -1 &&
                errno != EBADF && errno != EINVAL)
            {
                serverLog(LL_WARNING, "Fail to fsync the AOF file: %s",strerror(errno));
            }
            if (job->fd_args.need_reclaim_cache) {
                if (reclaimFilePageCache(job->fd_args.fd, 0, 0) == -1) {
                    serverLog(LL_NOTICE,"Unable to reclaim page cache: %s", strerror(errno));
                }
            }
            close(job->fd_args.fd);
        } else if (job_type == BIO_AOF_FSYNC || job_type == BIO_CLOSE_AOF) {
            /* fd 可能已被主线程关闭并被另一个 socket、管道或文件复用。
             * 这里直接忽略这些 errno，因为 AOF fsync 实际上并未真正失败。 */
            if (redis_fsync(job->fd_args.fd) == -1 &&
                errno != EBADF && errno != EINVAL)
            {
                int last_status;
                atomicGet(server.aof_bio_fsync_status,last_status);
                atomicSet(server.aof_bio_fsync_status,C_ERR);
                atomicSet(server.aof_bio_fsync_errno,errno);
                if (last_status == C_OK) {
                    serverLog(LL_WARNING,
                        "Fail to fsync the AOF file: %s",strerror(errno));
                }
            } else {
                atomicSet(server.aof_bio_fsync_status,C_OK);
                atomicSet(server.fsynced_reploff_pending, job->fd_args.offset);
            }

            if (job->fd_args.need_reclaim_cache) {
                if (reclaimFilePageCache(job->fd_args.fd, 0, 0) == -1) {
                    serverLog(LL_NOTICE,"Unable to reclaim page cache: %s", strerror(errno));
                }
            }
            // BIO_CLOSE_AOF 在 fsync 之后还需要关闭 fd
            if (job_type == BIO_CLOSE_AOF)
                close(job->fd_args.fd);
        } else if (job_type == BIO_LAZY_FREE) {
            // 惰性释放：调用注册的释放函数并传入参数数组
            job->free_args.free_fn(job->free_args.free_args);
        } else if ((job_type == BIO_COMP_RQ_CLOSE_FILE) ||
                   (job_type == BIO_COMP_RQ_AOF_FSYNC) ||
                   (job_type == BIO_COMP_RQ_LAZY_FREE)) {
            // 作业完成回调请求：构造响应项并放入完成列表，再通过管道唤醒主线程
            bio_comp_item *comp_rsp = zmalloc(sizeof(bio_comp_item));
            comp_rsp->func = job->comp_rq.fn;
            comp_rsp->arg = job->comp_rq.arg;

            /* 仅将其写入完成作业响应列表 */
            pthread_mutex_lock(&bio_mutex_comp);
            listAddNodeTail(bio_comp_list, comp_rsp);
            pthread_mutex_unlock(&bio_mutex_comp);

            if (write(job_comp_pipe[1],"A",1) != 1) {
                /* 管道是非阻塞的，若缓冲区已满 write() 可能失败。 */
            }
        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        zfree(job);

        /* 再次加锁以进入下一轮循环；若已无作业可处理，
         * 将在 pthread_cond_wait() 中再次阻塞。 */
        pthread_mutex_lock(&bio_mutex[worker]);
        listDelNode(bio_jobs[worker], ln);
        bio_jobs_counter[job_type]--;
        pthread_cond_signal(&bio_newjob_cond[worker]);
    }
}

/* 返回指定类型的待处理作业数。
 * 常用于 aof_pending_bio_fsync 等 INFO 字段以及 lazyfree 等待逻辑。 */
unsigned long bioPendingJobsOfType(int type) {
    unsigned int worker = bio_job_to_worker[type];

    pthread_mutex_lock(&bio_mutex[worker]);
    unsigned long val = bio_jobs_counter[type];
    pthread_mutex_unlock(&bio_mutex[worker]);

    return val;
}

/* 等待负责指定作业类型的工作线程队列清空。
 * 通过条件变量阻塞，直到该工作线程的作业队列长度为 0。 */
void bioDrainWorker(int job_type) {
    unsigned long worker = bio_job_to_worker[job_type];

    pthread_mutex_lock(&bio_mutex[worker]);
    while (listLength(bio_jobs[worker]) > 0) {
        pthread_cond_wait(&bio_newjob_cond[worker], &bio_mutex[worker]);
    }
    pthread_mutex_unlock(&bio_mutex[worker]);
}

/* 以一种“非常规”的方式终止正在运行的后台 I/O 线程。
 * 该函数仅在必须立即停止线程的紧急情况下使用。
 * 目前 Redis 仅在崩溃时（例如收到 SIGSEGV）调用它，
 * 以便在不受其他线程干扰的情况下执行快速内存检查。 */
void bioKillThreads(void) {
    int err;
    unsigned long j;

    for (j = 0; j < BIO_WORKER_NUM; j++) {
        // 不要取消当前线程自身
        if (bio_threads[j] == pthread_self()) continue;
        if (bio_threads[j] && pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "Bio worker thread #%lu can not be joined: %s",
                        j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "Bio worker thread #%lu terminated",j);
            }
        }
    }
}

/* 事件循环中管道读事件回调：从管道读端清空数据，
 * 然后取出 bio_comp_list 中的完成回调项并依次执行。
 * 通过在取出时整体替换列表的方式，避免长时间持锁。 */
void bioPipeReadJobCompList(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    char buf[128];
    list *tmp_list = NULL;

    // 排空管道中所有待读字节（仅用于唤醒，自身内容无意义）
    while (read(fd, buf, sizeof(buf)) == sizeof(buf));

    /* 如果管道由事件循环 API 写入，处理事件循环事件 */
    pthread_mutex_lock(&bio_mutex_comp);
    if (listLength(bio_comp_list)) {
        // 取出当前完成列表并替换为一个空列表，降低持锁时间
        tmp_list = bio_comp_list;
        bio_comp_list = listCreate();
    }
    pthread_mutex_unlock(&bio_mutex_comp);

    if (!tmp_list) return;

    /* 依次回调所有作业完成通知 */
    while (listLength(tmp_list)) {
        listNode *ln = listFirst(tmp_list);
        bio_comp_item *rsp = ln->value;
        listDelNode(tmp_list, ln);
        rsp->func(rsp->arg);
        zfree(rsp);
    }
    listRelease(tmp_list);
}
