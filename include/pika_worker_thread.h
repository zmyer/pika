// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_WORKER_THREAD_H_
#define PIKA_WORKER_THREAD_H_

#include <queue>

#include "pika_define.h"
#include "pika_client_conn.h"
#include "pika_command.h"
#include "../third/pink/include/worker_thread.h"
#include "../third/slash/include/env.h"

// TODO: 17/3/2 by zmyer
class PikaWorkerThread : public pink::WorkerThread<PikaClientConn> {
public:
    PikaWorkerThread(int cron_interval = 0);

    virtual ~PikaWorkerThread();

    virtual void CronHandle();

    int64_t ThreadClientList(std::vector<ClientInfo> *clients = NULL);

    bool ThreadClientKill(std::string ip_port = "");

    int ThreadClientNum();

    uint64_t thread_querynum() {
        slash::RWLock(&rwlock_, false);
        return thread_querynum_;
    }

    void ResetThreadQuerynum() {
        slash::RWLock(&rwlock_, true);
        thread_querynum_ = 0;
        last_thread_querynum_ = 0;
    }

    uint64_t last_sec_thread_querynum() {
        slash::RWLock(&rwlock_, false);
        return last_sec_thread_querynum_;
    }

    void PlusThreadQuerynum() {
        slash::RWLock(&rwlock_, true);
        thread_querynum_++;
    }

    // TODO: 17/3/5 by zmyer
    void ResetLastSecQuerynum() {
        //记录当前的时间
        uint64_t cur_time_us = slash::NowMicros();
        //加锁
        slash::RWLock l(&rwlock_, true);
        //计算线程的查询速率
        last_sec_thread_querynum_ = ((thread_querynum_ - last_thread_querynum_) * 1000000 /
                                     (cur_time_us - last_time_us_ + 1));
        //更新线程查询次数
        last_thread_querynum_ = thread_querynum_;
        //更新时间戳
        last_time_us_ = cur_time_us;
    }

    Cmd *GetCmd(const std::string &opt) {
        return GetCmdFromTable(opt, cmds_);
    }

private:
    slash::Mutex mutex_; // protect cron_task_
    std::queue<WorkerCronTask> cron_tasks_;

    uint64_t thread_querynum_;
    uint64_t last_thread_querynum_;
    uint64_t last_time_us_;
    uint64_t last_sec_thread_querynum_;

    std::unordered_map<std::string, Cmd *> cmds_;

    void AddCronTask(WorkerCronTask task);

    bool FindClient(std::string ip_port);

    void ClientKill(std::string ip_port);

    void ClientKillAll();
};

#endif
