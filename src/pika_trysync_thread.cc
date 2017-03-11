// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <poll.h>
#include "../include/pika_server.h"
#include "../include/pika_conf.h"
#include "../third/glog/src/glog/log_severity.h"
#include "../third/glog/src/windows/glog/logging.h"
#include "../third/slash/include/rsync.h"

//全局服务器对象
extern PikaServer *g_pika_server;
//全局服务器配置
extern PikaConf *g_pika_conf;

// TODO: 17/3/4 by zmyer
PikaTrysyncThread::~PikaTrysyncThread() {
    should_exit_ = true;
    //等待线程结束
    pthread_join(thread_id(), NULL);
    //关闭同步流程
    slash::StopRsync(g_pika_conf->db_sync_path());
    delete cli_;
    LOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
}

// TODO: 17/3/4 by zmyer
bool PikaTrysyncThread::Send() {
    pink::RedisCmdArgsType argv;
    std::string wbuf_str;
    //读取主节点的认证路径
    std::string masterauth = g_pika_conf->masterauth();
    //读取密码
    std::string requirepass = g_pika_conf->requirepass();
    if (masterauth != "") {
        argv.push_back("auth");
        argv.push_back(masterauth);
        //解析相关的指令
        pink::RedisCli::SerializeCommand(argv, &wbuf_str);
    } else if (requirepass != "") {
        argv.push_back("auth");
        argv.push_back(requirepass);
        //解析相关指令
        pink::RedisCli::SerializeCommand(argv, &wbuf_str);
    }

    //清理参数
    argv.clear();
    std::string tbuf_str;
    //首先插入指令名称
    argv.push_back("trysync");
    //插入服务器的主机名
    argv.push_back(g_pika_server->host());
    //插入服务器的端口号
    argv.push_back(std::to_string(g_pika_server->port()));
    //文件数量
    uint32_t filenum;
    //文件偏移量
    uint64_t pro_offset;
    //读取指定文件对应的producer对象的状态
    g_pika_server->logger_->GetProducerStatus(&filenum, &pro_offset);

    argv.push_back(std::to_string(filenum));
    argv.push_back(std::to_string(pro_offset));
    //开始序列化指令
    pink::RedisCli::SerializeCommand(argv, &tbuf_str);

    wbuf_str.append(tbuf_str);
    LOG(INFO) << wbuf_str;

    pink::Status s;
    //向服务器发送指令
    s = cli_->Send(&wbuf_str);
    if (!s.ok()) {
        LOG(WARNING) << "Connect master, Send, error: " << strerror(errno);
        return false;
    }
    return true;
}

// TODO: 17/3/4 by zmyer
bool PikaTrysyncThread::RecvProc() {
    //首先读取是否需要认证
    bool should_auth = g_pika_conf->requirepass() == "" ? false : true;
    bool is_authed = false;
    pink::Status s;
    std::string reply;

    while (1) {
        //读取应答消息
        s = cli_->Recv(NULL);
        if (!s.ok()) {
            LOG(WARNING) << "Connect master, Recv, error: " << strerror(errno);
            return false;
        }

        //读取应答消息
        reply = cli_->argv_[0];
        LOG(WARNING) << "Reply from master after trysync: " << reply;
        if (!is_authed && should_auth) {
            if (kInnerReplOk != slash::StringToLower(reply)) {
                LOG(WARNING) << "auth with master, error, come in SyncError stage";
                g_pika_server->SyncError();
                return false;
            }
            is_authed = true;
        } else {
            if (cli_->argv_.size() == 1 &&
                slash::string2l(reply.data(), reply.size(), (long *) &sid_)) {
                // Luckly, I got your point, the sync is comming
                LOG(INFO) << "Recv sid from master: " << sid_;
                break;
            }
            // Failed

            if (kInnerReplWait == reply) {
                // You can't sync this time, but may be different next time,
                // This may happened when
                // 1, Master do bgsave first.
                // 2, Master waiting for an existing bgsaving process
                // 3, Master do dbsyncing
                LOG(INFO) << "Need wait to sync";
                //等待同步
                g_pika_server->NeedWaitDBSync();
            } else {
                LOG(WARNING) << "something wrong with sync, come in SyncError stage";
                g_pika_server->SyncError();
            }
            return false;
        }
    }
    return true;
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaTrysyncThread cron will connect and do slaveof task with master
// TODO: 17/3/4 by zmyer
bool PikaTrysyncThread::TryUpdateMasterOffset() {
    // Check dbsync finished
    //读取同步线程的信息
    std::string info_path = g_pika_conf->db_sync_path() + kBgsaveInfoFile;
    if (!slash::FileExists(info_path)) {
        return false;
    }

    // Got new binlog offset
    //读取信息文件
    std::ifstream is(info_path);
    if (!is) {
        LOG(WARNING) << "Failed to open info file after db sync";
        return false;
    }
    std::string line, master_ip;
    int lineno = 0;
    int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
    while (std::getline(is, line)) {
        lineno++;
        if (lineno == 2) {
            master_ip = line;
        } else if (lineno > 2 && lineno < 6) {
            if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
                LOG(WARNING) << "Format of info file after db sync error, line : " << line;
                is.close();
                return false;
            }
            if (lineno == 3) { master_port = tmp; }
            else if (lineno == 4) { filenum = tmp; }
            else { offset = tmp; }

        } else if (lineno > 5) {
            LOG(WARNING) << "Format of info file after db sync error, line : " << line;
            is.close();
            return false;
        }
    }
    is.close();
    LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

    // Sanity check
    if (master_ip != g_pika_server->master_ip() ||
        master_port != g_pika_server->master_port()) {
        LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port;
        return false;
    }

    // Replace the old db
    slash::StopRsync(g_pika_conf->db_sync_path());
    slash::DeleteFile(info_path);
    if (!g_pika_server->ChangeDb(g_pika_conf->db_sync_path())) {
        LOG(WARNING) << "Failed to change db";
        return false;
    }

    // Update master offset
    g_pika_server->logger_->SetProducerStatus(filenum, offset);
    g_pika_server->WaitDBSyncFinish();
    return true;
}

// TODO: 17/3/4 by zmyer
void PikaTrysyncThread::PrepareRsync() {
    //读取同步目录
    std::string db_sync_path = g_pika_conf->db_sync_path();
    //首先停止在指定目录下的同步操作
    slash::StopRsync(db_sync_path);
    //在同步目录下创建各自集合子目录
    slash::CreatePath(db_sync_path + "kv");
    slash::CreatePath(db_sync_path + "hash");
    slash::CreatePath(db_sync_path + "list");
    slash::CreatePath(db_sync_path + "set");
    slash::CreatePath(db_sync_path + "zset");
}

// TODO maybe use RedisCli
// TODO: 17/3/4 by zmyer
void *PikaTrysyncThread::ThreadMain() {
    while (!should_exit_) {
        //等待片刻
        sleep(1);
        if (g_pika_server->WaitingDBSync()) {
            //Try to update offset by db sync
            //如果依旧在等待db同步,则尝试更新master节点的偏移量
            if (TryUpdateMasterOffset()) {
                LOG(INFO) << "Success Update Master Offset";
            }
        }

        //开始连接master节点
        if (!g_pika_server->ShouldConnectMaster()) {
            continue;
        }
        //等待片刻
        sleep(2);
        LOG(INFO) << "Should connect master";

        //读取master节点的ip
        std::string master_ip = g_pika_server->master_ip();
        //读取master节点的端口号
        int master_port = g_pika_server->master_port();

        // Start rsync
        //读取同步路径
        std::string dbsync_path = g_pika_conf->db_sync_path();
        //同步之前的准备工作
        PrepareRsync();
        //组装master节点的地址信息
        std::string ip_port = slash::IpPortString(master_ip, master_port);
        // We append the master ip port after module name
        // To make sure only data from current master is received
        //开始同步数据
        int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, g_pika_server->host(),
                                    g_pika_conf->port() + 3000);
        if (0 != ret) {
            LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
        }
        LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;

        //开始连接master节点
        if ((cli_->Connect(master_ip, master_port, g_pika_server->host())).ok()) {
            //设置客户端的发送超时时间
            cli_->set_send_timeout(30000);
            //设置客户端接收超时时间
            cli_->set_recv_timeout(30000);
            if (Send() && RecvProc()) {
                //如果发送和接收应答都正常,则设置连接master节点已经成功
                g_pika_server->ConnectMasterDone();
                // Stop rsync, binlog sync with master is begin
                //停止同步流程
                slash::StopRsync(dbsync_path);
                //删除ping线程
                delete g_pika_server->ping_thread_;
                //创建slave节点的ping线程
                g_pika_server->ping_thread_ = new PikaSlavepingThread(sid_);
                //重启ping线程
                g_pika_server->ping_thread_->StartThread();
                LOG(INFO) << "Trysync success";
            }
            //关闭客户端
            cli_->Close();
        } else {
            LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
        }
    }
    return NULL;
}
