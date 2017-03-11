// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <utility>
#include <sys/time.h>

#include "../include/pika_server.h"
#include "../include/pika_conf.h"
#include "../include/pika_monitor_thread.h"
#include "../third/glog/src/windows/glog/logging.h"

//全局服务器
extern PikaServer *g_pika_server;
//全局配置
extern PikaConf *g_pika_conf;

// TODO: 17/3/5 by zmyer
PikaMonitorThread::PikaMonitorThread() :
        pink::Thread(),
        monitor_cond_(&monitor_mutex_protector_),
        is_running_(false),
        should_exit_(false) {
}

// TODO: 17/3/5 by zmyer
PikaMonitorThread::~PikaMonitorThread() {
    should_exit_ = true;
    if (is_running_) {
        //向监听器条件变量发送信号
        monitor_cond_.SignalAll();
        //等待线程结束
        pthread_join(thread_id(), NULL);
    }
    for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
         iter != monitor_clients_.end(); ++iter) {
        //关闭客户端
        close(iter->fd);
    }
    LOG(INFO) << " PikaMonitorThread " << pthread_self() << " exit!!!";
}

// TODO: 17/3/5 by zmyer
void PikaMonitorThread::AddMonitorClient(pink::RedisConn *client_ptr) {
    //启动线程
    StartThread();
    //加锁
    slash::MutexLock lm(&monitor_mutex_protector_);
    //向监视器客户端列表中插入客户端
    monitor_clients_.push_back(ClientInfo{client_ptr->fd(), client_ptr->ip_port(), 0});
}

// TODO: 17/3/5 by zmyer
void PikaMonitorThread::RemoveMonitorClient(const std::string &ip_port) {
    std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
    for (; iter != monitor_clients_.end(); ++iter) {
        if (ip_port == "all") {
            //关闭客户端描述符
            close(iter->fd);
            continue;
        }
        if (iter->ip_port == ip_port) {
            //直接关闭指定的客户端
            close(iter->fd);
            break;
        }
    }
    if (ip_port == "all") {
        //清空所有的监视器客户端列表
        monitor_clients_.clear();
    } else if (iter != monitor_clients_.end()) {
        //从客户端列表中删除客户端
        monitor_clients_.erase(iter);
    }
}

// TODO: 17/3/5 by zmyer
void PikaMonitorThread::AddMonitorMessage(const std::string &monitor_message) {
    //加锁
    slash::MutexLock lm(&monitor_mutex_protector_);
    if (monitor_messages_.empty() && cron_tasks_.empty()) {
        //向监视器消息队列中插入监视器消息
        monitor_messages_.push_back(monitor_message);
        //监视器条件变量发送信号
        monitor_cond_.Signal();
    } else {
        //插入消息
        monitor_messages_.push_back(monitor_message);
    }
}

// TODO: 17/3/5 by zmyer
int32_t PikaMonitorThread::ThreadClientList(std::vector<ClientInfo> *clients_ptr) {
    if (clients_ptr != NULL) {
        for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
             iter != monitor_clients_.end(); iter++) {
            clients_ptr->push_back(*iter);
        }
    }
    return monitor_clients_.size();
}

// TODO: 17/3/5 by zmyer
void PikaMonitorThread::AddCronTask(MonitorCronTask task) {
    slash::MutexLock lm(&monitor_mutex_protector_);
    if (monitor_messages_.empty() && cron_tasks_.empty()) {
        //向定时任务队列中插入任务对象
        cron_tasks_.push(task);
        //向监视器条件变量发送信号
        monitor_cond_.Signal();
    } else {
        //插入任务对象
        cron_tasks_.push(task);
    }
}

// TODO: 17/3/5 by zmyer
bool PikaMonitorThread::FindClient(const std::string &ip_port) {
    slash::MutexLock lm(&monitor_mutex_protector_);
    for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
         iter != monitor_clients_.end(); ++iter) {
        if (iter->ip_port == ip_port) {
            return true;
        }
    }
    return false;
}

// TODO: 17/3/5 by zmyer
bool PikaMonitorThread::ThreadClientKill(const std::string &ip_port) {
    if (ip_port == "all") {
        //插入定时任务
        AddCronTask({TASK_KILLALL, "all"});
    } else if (FindClient(ip_port)) {
        //插入定时任务
        AddCronTask({TASK_KILL, ip_port});
    } else {
        return false;
    }
    return true;
}

// TODO: 17/3/5 by zmyer
bool PikaMonitorThread::HasMonitorClients() {
    slash::MutexLock lm(&monitor_mutex_protector_);
    return !monitor_clients_.empty();
}

// TODO: 17/3/5 by zmyer
void PikaMonitorThread::StartThread() {
    {
        //加锁
        slash::MutexLock lm(&monitor_mutex_protector_);
        if (is_running_) {
            //如果监视线程已经启动,则直接退出
            return;
        }
        //设置启动标记
        is_running_ = true;
    }
    //启动线程
    pink::Thread::StartThread();
}

// TODO: 17/3/5 by zmyer
pink::WriteStatus PikaMonitorThread::SendMessage(int32_t fd, std::string &message) {
    ssize_t nwritten = 0, message_len_sended = 0, message_len_left = message.size();
    while (message_len_left > 0) {
        //向描述符写入消息
        nwritten = write(fd, message.data() + message_len_sended, (size_t) message_len_left);
        if (nwritten == -1 && errno == EAGAIN) {
            continue;
        } else if (nwritten == -1) {
            return pink::kWriteError;
        }
        //更新已发送的字节数
        message_len_sended += nwritten;
        //更新待发送的字节数
        message_len_left -= nwritten;
    }
    return pink::kWriteAll;
}

// TODO: 17/3/5 by zmyer
void *PikaMonitorThread::ThreadMain() {
    //消息队列
    std::deque<std::string> messages_deque;
    //消息转发对象
    std::string messages_transfer;
    //监视器定时任务
    MonitorCronTask task;
    //写入状态对象
    pink::WriteStatus write_status;
    while (!should_exit_) {
        {
            //加锁
            slash::MutexLock lm(&monitor_mutex_protector_);
            while (monitor_messages_.empty() && cron_tasks_.empty() && !should_exit_) {
                //如果监视器消息队列为空并且定时任务队列为空并且线程没有退出,则在监视器条件变量上等待
                monitor_cond_.Wait();
            }
        }
        if (should_exit_) {
            //退出线程
            break;
        }
        {
            //加锁
            slash::MutexLock lm(&monitor_mutex_protector_);
            while (!cron_tasks_.empty()) {
                //从定时任务队列取出定时任务
                task = cron_tasks_.front();
                //删除定时任务 
                cron_tasks_.pop();
                //删除当前定时任务对应的客户端
                RemoveMonitorClient(task.ip_port);
                if (TASK_KILLALL) {
                    std::queue<MonitorCronTask> empty_queue;
                    //交换定时任务队列
                    cron_tasks_.swap(empty_queue);
                }
            }
        }
        //清理定时消息队列
        messages_deque.clear();
        {
            //加锁
            slash::MutexLock lm(&monitor_mutex_protector_);
            //交换监视器消息队列
            messages_deque.swap(monitor_messages_);
            if (monitor_clients_.empty() || messages_deque.empty()) {
                continue;
            }
        }

        messages_transfer = "+";
        for (std::deque<std::string>::iterator iter = messages_deque.begin();
             iter != messages_deque.end();
             ++iter) {
            //开始拼接消息队列中的消息
            messages_transfer.append(iter->data(), iter->size());
            messages_transfer.append("\n");
        }
        if (messages_transfer == "+") {
            continue;
        }
        //符号替换
        messages_transfer.replace(messages_transfer.size() - 1, 1, "\r\n", 0, 2);
        monitor_mutex_protector_.Lock();
        for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
             iter != monitor_clients_.end(); ++iter) {
            //开始发送消息
            write_status = SendMessage(iter->fd, messages_transfer);
            if (write_status == pink::kWriteError) {
                //如果出现发送消息错误,则直接将杀死客户端消息插入到定时任务队列中
                cron_tasks_.push({TASK_KILL, iter->ip_port});
            }
        }
        //解锁
        monitor_mutex_protector_.Unlock();
    }
    return NULL;
}
