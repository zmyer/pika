// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../third/slash/include/slash_status.h"
#include "../third/pink/include/redis_cli.h"
#include "../include/pika_server.h"
#include "../third/glog/src/windows/glog/logging.h"

using slash::Status;
using slash::Slice;
using pink::RedisCli;

//全局服务器对象
extern PikaServer *g_pika_server;

// TODO: 17/3/4 by zmyer
PikaBinlogSenderThread::PikaBinlogSenderThread(std::string &ip, int port, slash::SequentialFile *queue,
                                               uint32_t filenum, uint64_t con_offset)
        : con_offset_(con_offset),
          filenum_(filenum),
          initial_offset_(0),
          end_of_buffer_offset_(kBlockSize),
          queue_(queue),
          backing_store_(new char[kBlockSize]),
          buffer_(),
          ip_(ip),
          port_(port) {
    //创建redis客户端对象
    cli_ = new RedisCli();
    //记录上一次记录的偏移量
    last_record_offset_ = con_offset % kBlockSize;
    //初始化读写锁对象
    pthread_rwlock_init(&rwlock_, NULL);
}

// TODO: 17/3/4 by zmyer
PikaBinlogSenderThread::~PikaBinlogSenderThread() {
    //设置线程退出标记
    should_exit_ = true;
    //等待线程结束
    pthread_join(thread_id(), NULL);
    //删除顺序文件对象
    delete queue_;
    //删除读写锁
    pthread_rwlock_destroy(&rwlock_);
    delete[] backing_store_;
    //删除redis客户端
    delete cli_;

    LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

// TODO: 17/3/4 by zmyer
int PikaBinlogSenderThread::trim() {
    slash::Status s;
    //读取开始块偏移量
    uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
    //移动块的偏移量
    s = queue_->Skip((con_offset_ / kBlockSize) * kBlockSize);
    //读取块内的偏移量
    uint64_t block_offset = con_offset_ % kBlockSize;
    uint64_t ret = 0;
    uint64_t res = 0;
    bool is_error = false;

    while (true) {
        if (res >= block_offset) {
            con_offset_ = start_block + res;
            break;
        }
        ret = get_next(is_error);
        if (is_error == true) {
            return -1;
        }
        res += ret;
    }
    last_record_offset_ = con_offset_ % kBlockSize;

    return 0;
}

// TODO: 17/3/4 by zmyer
uint64_t PikaBinlogSenderThread::get_next(bool &is_error) {
    uint64_t offset = 0;
    slash::Status s;
    is_error = false;

    while (true) {
        //清理内存块
        buffer_.clear();
        //从顺序文件中读取指定的大小的内容
        s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
        //如果读取失败,则设置出错标记
        if (!s.ok()) {
            is_error = true;
        }

        //读取头部信息
        const char *header = buffer_.data();
        //读取头部信息第一个字节内容
        const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
        //读取第二个字节内容
        const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
        //读取第三个字节内容
        const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
        //读取类型信息
        const unsigned int type = header[7];
        //读取消息长度
        const uint32_t length = a | (b << 8) | (c << 16);

        //读取消息类型
        if (type == kFullType) {
            //读取指定长度的文件内容
            s = queue_->Read(length, &buffer_, backing_store_);
            //设置文件读取之后的偏移量
            offset += kHeaderSize + length;
            break;
        } else if (type == kFirstType) {
            //读取指定长度的文件内容
            s = queue_->Read(length, &buffer_, backing_store_);
            //更新偏移量
            offset += kHeaderSize + length;
        } else if (type == kMiddleType) {
            //读取指定长度的文件内容
            s = queue_->Read(length, &buffer_, backing_store_);
            //更新偏移量
            offset += kHeaderSize + length;
        } else if (type == kLastType) {
            //读取指定长度的文件内容
            s = queue_->Read(length, &buffer_, backing_store_);
            //更新偏移量
            offset += kHeaderSize + length;
            break;
        } else {
            is_error = true;
            break;
        }
    }
    //返回更新之后的偏移量
    return offset;
}

// TODO: 17/3/4 by zmyer
unsigned int PikaBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
    slash::Status s;
    if (end_of_buffer_offset_ - last_record_offset_ <= kHeaderSize) {
        //如果不够一个头部长度,则直接跳过
        queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
        //更新偏移量
        con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
        //更新最近一次记录的偏移量
        last_record_offset_ = 0;
    }
    //清理缓冲区
    buffer_.clear();
    //开始读取消息头部信息
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (s.IsEndFile()) {
        //如果已经达到了文件末尾,则直接返回结束标记
        return kEof;
    } else if (!s.ok()) {
        //读取失败,则直接返回错误的记录
        return kBadRecord;
    }

    //读取记录的头部内容
    const char *header = buffer_.data();
    //读取头部内容的第一个字节
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    //读取头部内容的第二个字节
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    //读取头部内容的第三个字节
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    //读取记录的类型
    const unsigned int type = (const unsigned int) header[7];
    //读取记录的长度
    const uint32_t length = a | (b << 8) | (c << 16);
    if (type == kZeroType || length == 0) {
        //如果记录的类型为zeroType或者记录长度为0,则清理缓冲区
        buffer_.clear();
        //返回做的旧的记录类型
        return kOldRecord;
    }

    //清理缓冲区
    buffer_.clear();
    //std::cout<<"2 --> con_offset_: "<<con_offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
    //读取记录文件
    s = queue_->Read(length, &buffer_, backing_store_);
    //根据读取的内容,创建记录对象
    *result = slash::Slice(buffer_.data(), buffer_.size());
    //更新最近一次记录的偏移量
    last_record_offset_ += kHeaderSize + length;
    if (s.ok()) {
        //更新确认的记录偏移量
        con_offset_ += (kHeaderSize + length);
    }
    //返回读取的记录类型
    return type;
}

// TODO: 17/3/4 by zmyer
Status PikaBinlogSenderThread::Consume(std::string &scratch) {
    Status s;
    if (last_record_offset_ < initial_offset_) {
        return slash::Status::IOError("last_record_offset exceed");
    }

    slash::Slice fragment;
    while (true) {
        //读取物理记录
        const unsigned int record_type = ReadPhysicalRecord(&fragment);

        switch (record_type) {
            case kFullType:
                //构造记录对象
                scratch = std::string(fragment.data(), fragment.size());
                //成功状态
                s = Status::OK();
                break;
            case kFirstType:
                //覆盖之前的记录对象
                scratch.assign(fragment.data(), fragment.size());
                //如果直接覆盖,则报错
                s = Status::NotFound("Middle Status");
                break;
            case kMiddleType:
                //追加记录
                scratch.append(fragment.data(), fragment.size());
                //报错
                s = Status::NotFound("Middle Status");
                break;
            case kLastType:
                //如果记录类型是lastType,则直接追加
                scratch.append(fragment.data(), fragment.size());
                //返回成功
                s = Status::OK();
                break;
            case kEof:
                //返回文件结束类型
                return Status::EndFile("Eof");
            case kBadRecord:
                //返回错误的记录类型
                return Status::IOError("Data Corruption");
            case kOldRecord:
                //返回文件结束类型
                return Status::EndFile("Eof");
            default:
                return Status::IOError("Unknow reason");
        }
        // TODO:do handler here
        if (s.ok()) {
            break;
        }
    }
    //DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
    return Status::OK();
}

// Get a whole message; 
// the status will be OK, IOError or Corruption;
// TODO: 17/3/4 by zmyer
Status PikaBinlogSenderThread::Parse(std::string &scratch) {
    scratch.clear();
    Status s;
    uint32_t pro_num;
    uint64_t pro_offset;
    //日志对象
    Binlog *logger = g_pika_server->logger_;
    while (!should_exit_) {
        //读取producer对象的状态
        logger->GetProducerStatus(&pro_num, &pro_offset);
        if (filenum_ == pro_num && con_offset_ == pro_offset) {
            //DLOG(INFO) << "BinlogSender Parse no new msg, filenum_" << filenum_ << ", con_offset " << con_offset_;
            usleep(10000);
            continue;
        }

        //DLOG(INFO) << "BinlogSender start Parse a msg               filenum_" << filenum_ << ", con_offset " << con_offset_;
        //开始消费记录
        s = Consume(scratch);

        //DLOG(INFO) << "BinlogSender after Parse a msg return " << s.ToString() << " filenum_" << filenum_ << ", con_offset " << con_offset_;
        if (s.IsEndFile()) {
            //如果当前的文件已经读取完毕,则需要重新创建一个日志文件
            std::string confile = NewFileName(g_pika_server->logger_->filename, filenum_ + 1);

            // Roll to next File
            if (slash::FileExists(confile)) {
                //如果文件已经存在,则需要删除
                DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
                //删除文件
                delete queue_;
                queue_ = NULL;

                //创建新的顺序文件对象
                slash::NewSequentialFile(confile, &(queue_));
                //递增日志文件数量
                filenum_++;
                //重置确认的日志文件偏移量
                con_offset_ = 0;
                //重置日志文件初始化偏移量
                initial_offset_ = 0;
                //设置缓冲区的结束偏移量
                end_of_buffer_offset_ = kBlockSize;
                //设置最近一次的记录的偏移量
                last_record_offset_ = con_offset_ % kBlockSize;
            } else {
                //如果文件还未创建成功,则先等待片刻
                usleep(10000);
            }
        } else {
            break;
        }
    }

    if (should_exit_) {
        return Status::Corruption("should exit");
    }
    return s;
}

// When we encount
// TODO: 17/3/4 by zmyer
void *PikaBinlogSenderThread::ThreadMain() {
    Status s;
    pink::Status result;
    bool last_send_flag = true;
    std::string scratch;
    //确保保留记录的空间
    scratch.reserve(1024 * 1024);

    while (!should_exit_) {
        //首先等待片刻
        sleep(1);
        // 1. Connect to slave
        //开始通过使用redis客户端连接服务器
        result = cli_->Connect(ip_, port_, g_pika_server->host());
        LOG(INFO) << "BinlogSender Connect slave(" << ip_ << ":" << port_ << ") " << result.ToString();

        if (result.ok()) {
            while (true) {
                // 2. Should Parse new msg;
                if (last_send_flag) {
                    //解析新的记录对象
                    s = Parse(scratch);
                    //DLOG(INFO) << "BinlogSender Parse, return " << s.ToString();

                    if (s.IsCorruption()) {     // should exit
                        LOG(WARNING) << "BinlogSender Parse failed, will exit, error: " << s.ToString();
                        //close(sockfd_);
                        break;
                    } else if (s.IsIOError()) {
                        LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
                        continue;
                    }
                }

                // 3. After successful parse, we send msg;
                //DLOG(INFO) << "BinlogSender Parse ok, filenum = " << filenum_ << ", con_offset = " << con_offset_;
                //开始向服务器发送消息
                result = cli_->Send(&scratch);
                if (result.ok()) {
                    //设置最近一次消息发送成功标记
                    last_send_flag = true;
                } else {
                    //发送失败
                    last_send_flag = false;
                    //close(sockfd_);
                    break;
                }
            }
        }

        // error
        //关闭客户端
        cli_->Close();
        //等待片刻
        sleep(1);
    }
    return NULL;
}

