// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_SENDER_THREAD_H_
#define PIKA_BINLOG_SENDER_THREAD_H_

namespace pink {
    class RedisCli;
}

// TODO: 17/3/4 by zmyer
class PikaBinlogSenderThread : public pink::Thread {
public:

    PikaBinlogSenderThread(std::string &ip, int port, slash::SequentialFile *queue, uint32_t filenum,
                           uint64_t con_offset);

    virtual ~PikaBinlogSenderThread();

    /*
     * Get and Set
     */
    // TODO: 17/3/4 by zmyer
    uint64_t last_record_offset() {
        slash::RWLock l(&rwlock_, false);
        return last_record_offset_;
    }

    // TODO: 17/3/4 by zmyer
    uint32_t filenum() {
        slash::RWLock l(&rwlock_, false);
        return filenum_;
    }

    // TODO: 17/3/4 by zmyer
    uint64_t con_offset() {
        slash::RWLock l(&rwlock_, false);
        return con_offset_;
    }

    int trim();

    uint64_t get_next(bool &is_error);


private:

    slash::Status Parse(std::string &scratch);

    slash::Status Consume(std::string &scratch);

    unsigned int ReadPhysicalRecord(slash::Slice *fragment);
    //记录块偏移量
    uint64_t con_offset_;
    uint32_t filenum_;

    uint64_t initial_offset_;
    uint64_t last_record_offset_;
    uint64_t end_of_buffer_offset_;

    //顺序文件对象
    slash::SequentialFile *queue_;
    char *const backing_store_;
    //缓冲区
    slash::Slice buffer_;

    std::string ip_;
    int port_;

    pthread_rwlock_t rwlock_;

    pink::RedisCli *cli_;

    virtual void *ThreadMain();
};

#endif
