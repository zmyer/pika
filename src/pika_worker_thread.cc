// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../include/pika_conf.h"
#include "../include/pika_worker_thread.h"
#include "../third/slash/include/env.h"
#include "../third/glog/src/windows/glog/log_severity.h"
#include "../third/glog/src/windows/glog/logging.h"

//全局配置
extern PikaConf *g_pika_conf;

// TODO: 17/3/2 by zmyer
PikaWorkerThread::PikaWorkerThread(int cron_interval) :
        pink::WorkerThread::WorkerThread(cron_interval),
        thread_querynum_(0),
        last_thread_querynum_(0),
        last_time_us_(slash::NowMicros()),
        last_sec_thread_querynum_(0) {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
}

// TODO: 17/3/5 by zmyer
PikaWorkerThread::~PikaWorkerThread() {
    should_exit_ = true;
    //等待线程结束
    pthread_join(thread_id(), NULL);
    DestoryCmdTable(cmds_);
    LOG(INFO) << "A worker thread " << thread_id() << " exit!!!";
}

// TODO: 17/3/5 by zmyer
void PikaWorkerThread::CronHandle() {
/*
 *  Reset Lastsecquerynum and find timeout client and add them to cron_tasks_ to kill them
 */
    //重置线程查询速率
    ResetLastSecQuerynum();
    {
        struct timeval now;
        gettimeofday(&now, NULL);
        slash::RWLock l(&rwlock_, false); // Use ReadLock to iterate the conns_
        std::map<int, void *>::iterator iter = conns_.begin();

        while (iter != conns_.end()) {
            if (now.tv_sec - static_cast<PikaClientConn *>(iter->second)->last_interaction().tv_sec >
                g_pika_conf->timeout()) {
                LOG(INFO) << "Find Timeout Client: " << static_cast<PikaClientConn *>(iter->second)->ip_port();
                //向定时任务队列插入杀死超时任务
                AddCronTask(WorkerCronTask{TASK_KILL, static_cast<PikaClientConn *>(iter->second)->ip_port()});
            }
            iter++;
        }
    }

/*
 *  do crontask
 */
    {
        WorkerCronTask t;
        //加锁
        slash::MutexLock l(&mutex_);

        while (!cron_tasks_.empty()) {
            //从定时队列中读取定时任务
            t = cron_tasks_.front();
            //删除定时任务
            cron_tasks_.pop();
            mutex_.Unlock();
            DLOG(INFO) << "PikaWorkerThread, Got a WorkerCronTask";
            switch (t.task) {
                case TASK_KILL:
                    //杀死指定的任务
                    ClientKill(t.ip_port);
                    break;
                case TASK_KILLALL:
                    //杀死所有的定时任务
                    ClientKillAll();
                    break;
            }
            mutex_.Lock();
        }
    }
}

// TODO: 17/3/5 by zmyer
bool PikaWorkerThread::ThreadClientKill(std::string ip_port) {

    if (ip_port == "") {
        //向定时任务队列中插入到任务对象
        AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
    } else {
        if (!FindClient(ip_port)) {
            return false;
        }
        //插入定时任务
        AddCronTask(WorkerCronTask{TASK_KILL, ip_port});
    }
    return true;
}

// TODO: 17/3/3 by zmyer
int PikaWorkerThread::ThreadClientNum() {
    slash::RWLock l(&rwlock_, false);
    //返回客户端链接数量
    return conns_.size();
}

// TODO: 17/3/5 by zmyer
void PikaWorkerThread::AddCronTask(WorkerCronTask task) {
    slash::MutexLock l(&mutex_);
    //向定时任务队列插入任务对象
    cron_tasks_.push(task);
}

// TODO: 17/3/5 by zmyer
bool PikaWorkerThread::FindClient(std::string ip_port) {
    slash::RWLock l(&rwlock_, false);
    std::map<int, void *>::iterator iter;
    for (iter = conns_.begin(); iter != conns_.end(); iter++) {
        if (static_cast<PikaClientConn *>(iter->second)->ip_port() == ip_port) {
            return true;
        }
    }
    return false;
}

// TODO: 17/3/5 by zmyer
void PikaWorkerThread::ClientKill(std::string ip_port) {
    slash::RWLock l(&rwlock_, true);
    std::map<int, void *>::iterator iter;
    for (iter = conns_.begin(); iter != conns_.end(); iter++) {
        if (static_cast<PikaClientConn *>(iter->second)->ip_port() != ip_port) {
            continue;
        }
        close(iter->first);
        delete (static_cast<PikaClientConn *>(iter->second));
        conns_.erase(iter);
        break;
    }
}

// TODO: 17/3/5 by zmyer
void PikaWorkerThread::ClientKillAll() {
    slash::RWLock l(&rwlock_, true);
    std::map<int, void *>::iterator iter = conns_.begin();
    while (iter != conns_.end()) {
        close(iter->first);
        delete (static_cast<PikaClientConn *>(iter->second));
        iter = conns_.erase(iter);
    }
}

// TODO: 17/3/5 by zmyer
int64_t PikaWorkerThread::ThreadClientList(std::vector<ClientInfo> *clients) {
    slash::RWLock l(&rwlock_, false);
    if (clients != NULL) {
        std::map<int, void *>::const_iterator iter = conns_.begin();
        while (iter != conns_.end()) {
            clients->push_back(ClientInfo{
                    iter->first, reinterpret_cast<PikaClientConn *>(iter->second)->ip_port(),
                    static_cast<int>((reinterpret_cast<PikaClientConn *>(iter->second)->last_interaction()).tv_sec)});
            iter++;
        }
    }
    return conns_.size();
}
