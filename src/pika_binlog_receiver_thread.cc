// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../include/pika_server.h"
#include "../third/glog/src/windows/glog/logging.h"
#include "../third/slash/include/slash_mutex.h"
//全局服务器
extern PikaServer *g_pika_server;

// TODO: 17/3/3 by zmyer
PikaBinlogReceiverThread::PikaBinlogReceiverThread(std::string &ip, int port, int cron_interval) :
        pink::HolyThread::HolyThread(ip, port, cron_interval),
        thread_querynum_(0),
        last_thread_querynum_(0),
        last_time_us_(slash::NowMicros()),
        last_sec_thread_querynum_(0),
        serial_(0) {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
}

// TODO: 17/3/3 by zmyer
PikaBinlogReceiverThread::PikaBinlogReceiverThread(std::set<std::string> &ips, int port, int cron_interval) :
        pink::HolyThread::HolyThread(ips, port, cron_interval),
        thread_querynum_(0),
        last_thread_querynum_(0),
        last_time_us_(slash::NowMicros()),
        last_sec_thread_querynum_(0),
        serial_(0) {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
}

// TODO: 17/3/3 by zmyer
PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {
    DestoryCmdTable(cmds_);
    LOG(INFO) << "BinlogReceiver thread " << thread_id() << " exit!!!";
}

// TODO: 17/3/3 by zmyer
bool PikaBinlogReceiverThread::AccessHandle(std::string &ip) {
    if (ip == "127.0.0.1") {
        //如果接受到的连接是本机,则设置本主机名
        ip = g_pika_server->host();
    }
    if (ThreadClientNum() != 0 || !g_pika_server->ShouldAccessConnAsMaster(ip)) {
        LOG(WARNING) << "BinlogReceiverThread AccessHandle failed: " << ip;
        return false;
    }
    //递增master节点的客户端连接数
    g_pika_server->PlusMasterConnection();
    return true;
}

// TODO: 17/3/3 by zmyer
void PikaBinlogReceiverThread::CronHandle() {
    //重置服务器查询数据
    ResetLastSecQuerynum();
    {
        WorkerCronTask t;
        //加锁
        slash::MutexLock l(&mutex_);

        while (!cron_tasks_.empty()) {
            //从定时任务队列中读取任务
            t = cron_tasks_.front();
            cron_tasks_.pop();
            //释放锁
            mutex_.Unlock();
            DLOG(INFO) << "PikaBinlogReceiverThread, Got a WorkerCronTask";
            //检查读取的任务对象状态
            switch (t.task) {
                case TASK_KILL:
                    break;
                case TASK_KILLALL:
                    KillAll();
                    break;
            }
            //加锁
            mutex_.Lock();
        }
    }
}

// TODO: 17/3/3 by zmyer
void PikaBinlogReceiverThread::KillBinlogSender() {
    //添加定时任务对象
    AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
}

// TODO: 17/3/3 by zmyer
void PikaBinlogReceiverThread::AddCronTask(WorkerCronTask task) {
    slash::MutexLock l(&mutex_);
    //将定时任务插入到定时队列中
    cron_tasks_.push(task);
}

// TODO: 17/3/3 by zmyer
void PikaBinlogReceiverThread::KillAll() {
    {
        slash::RWLock l(&rwlock_, true);
        std::map<int, void *>::iterator iter = conns_.begin();
        while (iter != conns_.end()) {
            LOG(INFO) << "==========Kill Master Sender Conn==============";
            //依次关闭每个链接描述符
            close(iter->first);
            //删除连接对象
            delete (static_cast<PikaMasterConn *>(iter->second));
            //从队列中删除连接对象
            iter = conns_.erase(iter);
        }
    }
    //递减master节点连接数
    g_pika_server->MinusMasterConnection();
}
