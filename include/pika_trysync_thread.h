// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TRYSYNC_THREAD_H_
#define PIKA_TRYSYNC_THREAD_H_

// TODO: 17/3/4 by zmyer
class PikaTrysyncThread : public pink::Thread {
public:
    // TODO: 17/3/4 by zmyer
    PikaTrysyncThread() {
        //创建客户端
        cli_ = new pink::RedisCli();
        //设置客户端连接超时时间
        cli_->set_connect_timeout(1500);
    };

    virtual ~PikaTrysyncThread();

private:
    //套接字描述符
    int sockfd_;
    //同步线程id
    int64_t sid_;
    //redis客户端
    pink::RedisCli *cli_;

    bool Send();

    bool RecvProc();

    void PrepareRsync();

    bool TryUpdateMasterOffset();

    virtual void *ThreadMain();

};

#endif
