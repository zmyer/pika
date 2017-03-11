// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sys/resource.h>
#include <sys/fcntl.h>
#include "../include/pika_conf.h"
#include "../include/pika_server.h"
#include "../third/slash/include/env.h"
#include "../third/glog/src/glog/log_severity.h"
#include "../third/glog/src/windows/glog/logging.h"
#include "../include/pika_slot.h"

//全局配置
PikaConf *g_pika_conf;
//全局服务器
PikaServer *g_pika_server;

// TODO: 17/3/2 by zmyer
static void version() {
    printf("-----------Pika server %s ----------\n", kPikaVersion.c_str());
}

// TODO: 17/3/2 by zmyer
static void PikaConfInit(const std::string &path) {
    printf("path : %s\n", path.c_str());
    //创建配置文件
    g_pika_conf = new PikaConf(path);
    //开始加载配置文件
    if (g_pika_conf->Load() != 0) {
        LOG(FATAL) << "pika load conf error";
    }
    //显示版本号
    version();
    printf("-----------Pika config list----------\n");
    //打印配置项
    g_pika_conf->DumpConf();
    printf("-----------Pika config end----------\n");
}

// TODO: 17/3/2 by zmyer
static void PikaGlogInit() {
    if (!slash::FileExists(g_pika_conf->log_path())) {
        //如果日志文件不存在,则直接创建
        slash::CreatePath(g_pika_conf->log_path());
    }

    if (!g_pika_conf->daemonize()) {
        //如果启动的是非守护模式,则开启标注错误输出
        FLAGS_alsologtostderr = true;
    }
    //读取日志存储目录
    FLAGS_log_dir = g_pika_conf->log_path();
    //读取日志等级
    FLAGS_minloglevel = g_pika_conf->log_level();
    //设置日志最大值
    FLAGS_max_log_size = 1800;
    //日志缓存的时间
    FLAGS_logbufsecs = 0;
    ::google::InitGoogleLogging("pika");
}

// TODO: 17/3/2 by zmyer
static void daemonize() {
    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */
}

// TODO: 17/3/2 by zmyer
static void close_std() {
    //关闭标准输入输出
    int fd;
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        close(fd);
    }
}

// TODO: 17/3/2 by zmyer
static void create_pid_file(void) {
    /* Try to write the pid file in a best-effort way. */
    //创建保存进程号的文件
    std::string path(g_pika_conf->pidfile());
    size_t pos = path.find_last_of('/');
    if (pos != std::string::npos) {
        // mkpath(path.substr(0, pos).c_str(), 0755);
        //首先先创建目录
        slash::CreateDir(path.substr(0, pos));
    } else {
        //直接在当前目录创建文件
        path = kPikaPidFile;
    }
    //开始打开文件,如果不存在,则创建
    FILE *fp = fopen(path.c_str(), "w");
    if (fp) {
        //写入当前的进程id
        fprintf(fp, "%d\n", (int) getpid());
        //关闭文件
        fclose(fp);
    }
}

// TODO: 17/3/2 by zmyer
static void IntSigHandle(const int sig) {
    LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
    //进程直接退出
    g_pika_server->Exit();
}

// TODO: 17/3/2 by zmyer
static void PikaSignalSetup() {
    //忽视HUP和PIPE信号
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, &IntSigHandle);
    signal(SIGQUIT, &IntSigHandle);
    signal(SIGTERM, &IntSigHandle);
}

static void usage() {
    fprintf(stderr,
            "Pika module %s\n"
                    "usage: pika [-hv] [-c conf/file]\n"
                    "\t-h               -- show this help\n"
                    "\t-c conf/file     -- config file \n"
                    "  example: ./output/bin/pika -c ./conf/pika.conf\n",
            kPikaVersion.c_str()
    );
}

// TODO: 17/3/2 by zmyer
int main(int argc, char *argv[]) {
    if (argc < 2) {
        usage();
        exit(-1);
    }

    bool path_opt = false;
    char c;
    char path[1024];
    while (-1 != (c = (char) getopt(argc, argv, "c:hv"))) {
        switch (c) {
            case 'c':
                snprintf(path, 1024, "%s", optarg);
                path_opt = true;
                break;
            case 'h':
                usage();
                return 0;
            case 'v':
                version();
                return 0;
            default:
                usage();
                return 0;
        }
    }

    if (!path_opt) {
        fprintf(stderr, "Please specify the conf file path\n");
        usage();
        exit(-1);
    }

    //初始化配置
    PikaConfInit(path);

    rlimit limit;
    //读取可打开的文件数目配置
    if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
        LOG(WARNING) << "getrlimit error: " << strerror(errno);
    } else if (limit.rlim_cur < static_cast<unsigned int>(g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS)) {
        rlim_t old_limit = limit.rlim_cur;
        rlim_t best_limit = (rlim_t) (g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS);
        limit.rlim_cur = best_limit > limit.rlim_max ? limit.rlim_max - 1 : best_limit;
        limit.rlim_max = best_limit > limit.rlim_max ? limit.rlim_max - 1 : best_limit;
        //更新可打开文件数目的配置
        if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
            LOG(WARNING) << "your 'limit -n ' of " << old_limit <<
            " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
        } else {
            LOG(FATAL) << "your 'limit -n ' of " << old_limit <<
            " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno) << "), do it by yourself";
        }
    }

    // daemonize if needed
    if (g_pika_conf->daemonize()) {
        //如果配置了守护模式,则启动后台守护进程
        daemonize();
        //并创建保存进程号的文件
        create_pid_file();
    }

    //初始化全局配置
    PikaGlogInit();
    //初始化信号处理机制
    PikaSignalSetup();
    //初始化指令表
    InitCmdInfoTable();
    //初始化循环校验表
    InitCRC32Table();

    LOG(INFO) << "Server at: " << path;
    //创建服务器对象
    g_pika_server = new PikaServer();
    //如果开启了守护模式,则需要关闭标准输入输出操作
    if (g_pika_conf->daemonize()) {
        close_std();
    }
    //启动服务器
    g_pika_server->Start();

    return 0;
}
