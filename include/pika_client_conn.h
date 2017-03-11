// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <atomic>
#include "pika_command.h"
#include "../third/pink/include/redis_conn.h"
#include "../third/pink/include/pink_thread.h"


class PikaWorkerThread;

// TODO: 17/3/5 by zmyer
class PikaClientConn : public pink::RedisConn {
public:
    PikaClientConn(int fd, std::string ip_port, pink::Thread *thread);

    virtual ~PikaClientConn();

    virtual int DealMessage();

    // TODO: 17/3/5 by zmyer
    PikaWorkerThread *self_thread() {
        return self_thread_;
    }

private:
    //线程对象
    PikaWorkerThread *self_thread_;

    std::string DoCmd(const std::string &opt);

    std::string RestoreArgs();

    // Auth related
    // TODO: 17/3/5 by zmyer
    class AuthStat {
    public:
        void Init();

        bool IsAuthed(const CmdInfo *const cinfo_ptr);

        bool ChecknUpdate(const std::string &arg);

    private:
        enum StatType {
            kNoAuthed = 0,
            kAdminAuthed,
            kLimitAuthed,
        };
        StatType stat_;
    };

    AuthStat auth_stat_;
};

#endif
