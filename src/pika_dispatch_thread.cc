// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../include/pika_server.h"
#include "../include/pika_conf.h"
#include "../third/glog/src/windows/glog/logging.h"

//全局服务器
extern PikaServer* g_pika_server;
//全局配置对象
extern PikaConf* g_pika_conf;

// TODO: 17/3/2 by zmyer
PikaDispatchThread::PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  pink::DispatchThread::DispatchThread(port, work_num, reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread), cron_interval) {
}

// TODO: 17/3/2 by zmyer
PikaDispatchThread::PikaDispatchThread(std::string &ip, int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  pink::DispatchThread::DispatchThread(ip, port, work_num, reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread), cron_interval) {
}

// TODO: 17/3/2 by zmyer
PikaDispatchThread::PikaDispatchThread(std::set<std::string> &ips, int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  pink::DispatchThread::DispatchThread(ips, port, work_num, reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread), cron_interval) {
}

// TODO: 17/3/2 by zmyer
PikaDispatchThread::~PikaDispatchThread() {
  LOG(INFO) << "dispatch thread " << thread_id() << " exit!!!";
}

// TODO: 17/3/2 by zmyer
bool PikaDispatchThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
      //读取服务器主机地址
    ip = g_pika_server->host();
  }

  //读取客户端连接数目
  int client_num = ClientNum();
  if ((client_num >= g_pika_conf->maxclients() + g_pika_conf->root_connection_num())
      || (client_num >= g_pika_conf->maxclients() && ip != g_pika_server->host())) {
    LOG(WARNING) << "Max connections reach, Deny new comming: " << ip;
    return false;
  }

  DLOG(INFO) << "new clinet comming, ip: " << ip;
    //递增连接数
  g_pika_server->incr_accumulative_connections();
  return true;
}

// TODO: 17/3/2 by zmyer
int PikaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
      //统计每个线程包含的客户端连接数量
    num += ((PikaWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
