// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../include/pika_heartbeat_thread.h"
#include "../include/pika_server.h"
#include "../third/glog/src/glog/log_severity.h"
#include "../third/glog/src/windows/glog/logging.h"
#include "../third/slash/include/slash_mutex.h"

//全局服务器对象
extern PikaServer *g_pika_server;

// TODO: 17/3/4 by zmyer
PikaHeartbeatThread::PikaHeartbeatThread(std::string &ip, int port, int cron_interval) :
        pink::HolyThread::HolyThread(ip, port, cron_interval) {
}

// TODO: 17/3/4 by zmyer
PikaHeartbeatThread::PikaHeartbeatThread(std::set<std::string> &ips, int port, int cron_interval) :
        pink::HolyThread::HolyThread(ips, port, cron_interval) {
}

// TODO: 17/3/4 by zmyer
PikaHeartbeatThread::~PikaHeartbeatThread() {
    LOG(INFO) << "PikaHeartbeat thread " << thread_id() << " exit!!!";
}

// TODO: 17/3/4 by zmyer
void PikaHeartbeatThread::CronHandle() {
/*
 *	find out timeout slave and kill them 
 */
    struct timeval now;
    gettimeofday(&now, NULL);
    {
        slash::RWLock l(&rwlock_, true); // Use WriteLock to iterate the conns_
        std::map<int, void *>::iterator iter = conns_.begin();
        while (iter != conns_.end()) {
            if (now.tv_sec - static_cast<PikaHeartbeatConn *>(iter->second)->last_interaction().tv_sec > 20) {
                LOG(INFO) << "Find Timeout Slave: " << static_cast<PikaHeartbeatConn *>(iter->second)->ip_port();
                //心跳超时,关闭指定的描述符
                close(iter->first);
                //	erase item in slaves_
                //删除slave节点
                g_pika_server->DeleteSlave(iter->first);
                //删除指定的连接对象
                delete (static_cast<PikaHeartbeatConn *>(iter->second));
                //从连接集合中删除连接对象
                iter = conns_.erase(iter);
                continue;
            }
            //递增迭代器
            iter++;
        }
    }

/*
 * find out: 1. stay STAGE_ONE too long
 *					 2. the hb_fd have already be deleted
 * erase it in slaves_;
 */
    {
        slash::MutexLock l(&g_pika_server->slave_mutex_);
        std::vector<SlaveItem>::iterator iter = g_pika_server->slaves_.begin();
        while (iter != g_pika_server->slaves_.end()) {
            DLOG(INFO) << "sid: " << iter->sid << " ip_port: " << iter->ip_port << " port " << iter->port <<
            " sender_tid: " << iter->sender_tid << " hb_fd: " << iter->hb_fd << " stage :" << iter->stage <<
            " sender: " << iter->sender << " create_time: " << iter->create_time.tv_sec;
            if ((iter->stage == SLAVE_ITEM_STAGE_ONE && now.tv_sec - iter->create_time.tv_sec > 30)
                || (iter->stage == SLAVE_ITEM_STAGE_TWO && !FindSlave(iter->hb_fd))) {
                //pthread_kill(iter->tid);

                // Kill BinlogSender
                LOG(WARNING) << "Erase slave " << iter->ip_port << " from slaves map of heartbeat thread";
                {
                    //TODO maybe bug here
                    g_pika_server->slave_mutex_.Unlock();
                    g_pika_server->DeleteSlave(iter->hb_fd);
                    g_pika_server->slave_mutex_.Lock();
                }
                continue;
            }
            iter++;
        }
    }
}

bool PikaHeartbeatThread::AccessHandle(std::string &ip) {
    if (ip == "127.0.0.1") {
        ip = g_pika_server->host();
    }
    slash::MutexLock l(&g_pika_server->slave_mutex_);
    std::vector<SlaveItem>::iterator iter = g_pika_server->slaves_.begin();
    while (iter != g_pika_server->slaves_.end()) {
        if (iter->ip_port.find(ip) != std::string::npos) {
            LOG(INFO) << "HeartbeatThread access connection " << ip;
            return true;
        }
        iter++;
    }
    LOG(WARNING) << "HeartbeatThread deny connection: " << ip;
    return false;
}

bool PikaHeartbeatThread::FindSlave(int fd) {
    slash::RWLock(&rwlock_, false);
    std::map<int, void *>::iterator iter;
    for (iter = conns_.begin(); iter != conns_.end(); iter++) {
        if (iter->first == fd) {
            return true;
        }
    }
    return false;
}

