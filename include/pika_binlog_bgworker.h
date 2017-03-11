// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_BGWORKER_H_
#define PIKA_BINLOG_BGWORKER_H_

#include "pika_command.h"
#include "../third/pink/include/bg_thread.h"

// TODO: 17/3/5 by zmyer
class BinlogBGWorker {
public:
    // TODO: 17/3/5 by zmyer
    BinlogBGWorker(int full) : binlogbg_thread_(full) {
        cmds_.reserve(300);
        InitCmdTable(&cmds_);
    }

    // TODO: 17/3/5 by zmyer
    ~BinlogBGWorker() {
        //停止binlog后台线程
        binlogbg_thread_.Stop();
        //销毁指令表
        DestoryCmdTable(cmds_);
    }

    // TODO: 17/3/5 by zmyer
    Cmd *GetCmd(const std::string &opt) {
        //从指令表中读取指令
        return GetCmdFromTable(opt, cmds_);
    }

    // TODO: 17/3/5 by zmyer
    void Schedule(PikaCmdArgsType *argv, const std::string &raw_args, uint64_t serial, bool readonly) {
        //binlog后台参数对象
        BinlogBGArg *arg = new BinlogBGArg(argv, raw_args, serial, readonly, this);
        //启动binlog后台线程
        binlogbg_thread_.StartIfNeed();
        //开始调度binlog后台线程任务
        binlogbg_thread_.Schedule(&DoBinlogBG, static_cast<void *>(arg));
    }

    // TODO: 17/3/5 by zmyer
    static void DoBinlogBG(void *arg);

private:
    //指令表
    std::unordered_map<std::string, Cmd *> cmds_;
    //binlog后台线程
    pink::BGThread binlogbg_thread_;

    // TODO: 17/3/5 by zmyer
    struct BinlogBGArg {
        //参数类型
        PikaCmdArgsType *argv;
        //原始参数值,该值为字符串
        std::string raw_args;
        uint64_t serial;
        bool readonly; // Server readonly status at the view of binlog dispatch thread
        //binlog后台工作线程
        BinlogBGWorker *myself;

        // TODO: 17/3/5 by zmyer
        BinlogBGArg(PikaCmdArgsType *_argv, const std::string &_raw, uint64_t _s,
                    bool _readonly, BinlogBGWorker *_my) :
                argv(_argv), raw_args(_raw), serial(_s), readonly(_readonly), myself(_my) { }
    };
};
#endif
