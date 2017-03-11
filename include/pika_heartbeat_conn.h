// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HEARTBEAT_CONN_H_
#define PIKA_HEARTBEAT_CONN_H_

#include "../third/pink/include/redis_conn.h"


class PikaHeartbeatThread;

// TODO: 17/3/5 by zmyer
class PikaHeartbeatConn : public pink::RedisConn {
public:
    PikaHeartbeatConn(int fd, std::string ip_port, pink::Thread *thread);

    virtual ~PikaHeartbeatConn();

    virtual int DealMessage();

private:
    //心跳线程
    PikaHeartbeatThread *pika_thread_;
};

#endif
