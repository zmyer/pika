// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef  PIKA_MONITOR_THREAD_H_
#define  PIKA_MONITOR_THREAD_H_

#include <map>
#include <set>
#include <atomic>
#include <list>
#include <deque>
#include <queue>

#include "pika_client_conn.h"
#include "pika_define.h"

// TODO: 17/3/5 by zmyer
class PikaMonitorThread : public pink::Thread {
public:
    PikaMonitorThread();

    virtual ~PikaMonitorThread();

    void AddMonitorClient(pink::RedisConn *client_ptr);

    void AddMonitorMessage(const std::string &monitor_message);

    int32_t ThreadClientList(std::vector<ClientInfo> *client = NULL);

    bool ThreadClientKill(const std::string &ip_port = "all");

    bool HasMonitorClients();

private:
    void AddCronTask(MonitorCronTask task);

    bool FindClient(const std::string &ip_port);

    pink::WriteStatus SendMessage(int32_t fd, std::string &message);

    void RemoveMonitorClient(const std::string &ip_port);

    //互斥量
    slash::Mutex monitor_mutex_protector_;
    //条件变量
    slash::CondVar monitor_cond_;
    //监视器客户列表
    std::list<ClientInfo> monitor_clients_;
    //监视器消息队列
    std::deque<std::string> monitor_messages_;
    //运行标记
    std::atomic<bool> is_running_;
    //退出标记
    std::atomic<bool> should_exit_;
    //定时任务队列
    std::queue<MonitorCronTask> cron_tasks_;

    virtual void *ThreadMain();

    void RemoveMonitorClient(int32_t client_fd);

    void StartThread();
};

#endif
