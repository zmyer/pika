// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CONF_H_
#define PIKA_CONF_H_

#include <pthread.h>
#include "stdlib.h"
#include <string>
#include <vector>

#include "pika_define.h"
#include "../third/slash/include/slash_mutex.h"
#include "../third/slash/include/slash_string.h"
#include "../third/slash/include/base_conf.h"

typedef slash::RWLock RWLock;

// TODO: 17/3/2 by zmyer
class PikaConf : public slash::BaseConf {
public:
    PikaConf(const std::string &path);

    ~PikaConf() { pthread_rwlock_destroy(&rwlock_); }

    // Getter
    //TODO: 17/3/5 by zmyer
    int port() {
        RWLock l(&rwlock_, false);
        return port_;
    }

    //TODO: 17/3/5 by zmyer
    std::string slaveof() {
        RWLock l(&rwlock_, false);
        return slaveof_;
    }

    //TODO: 17/3/5 by zmyer
    int thread_num() {
        RWLock l(&rwlock_, false);
        return thread_num_;
    }

    //TODO: 17/3/5 by zmyer
    int sync_thread_num() {
        RWLock l(&rwlock_, false);
        return sync_thread_num_;
    }

    //TODO: 17/3/5 by zmyer
    int sync_buffer_size() {
        RWLock l(&rwlock_, false);
        return sync_buffer_size_;
    }

    //TODO: 17/3/5 by zmyer
    std::string log_path() {
        RWLock l(&rwlock_, false);
        return log_path_;
    }

    //TODO: 17/3/5 by zmyer
    int log_level() {
        RWLock l(&rwlock_, false);
        return log_level_;
    }

    // TODO: 17/3/2 by zmyer
    std::string db_path() {
        RWLock l(&rwlock_, false);
        return db_path_;
    }
    // TODO: 17/3/2 by zmyer
    std::string db_sync_path() {
        RWLock l(&rwlock_, false);
        return db_sync_path_;
    }

    //TODO: 17/3/5 by zmyer
    int db_sync_speed() {
        RWLock l(&rwlock_, false);
        return db_sync_speed_;
    }

    //TODO: 17/3/5 by zmyer
    int write_buffer_size() {
        RWLock l(&rwlock_, false);
        return write_buffer_size_;
    }

    //TODO: 17/3/5 by zmyer
    int timeout() {
        RWLock l(&rwlock_, false);
        return timeout_;
    }

    //TODO: 17/3/5 by zmyer
    std::string requirepass() {
        RWLock l(&rwlock_, false);
        return requirepass_;
    }

    //TODO: 17/3/5 by zmyer
    std::string masterauth() {
        RWLock l(&rwlock_, false);
        return masterauth_;
    }

    //TODO: 17/3/5 by zmyer
    bool slotmigrate() {
        RWLock l(&rwlock_, false);
        return slotmigrate_;
    }

    //TODO: 17/3/5 by zmyer
    std::string bgsave_path() {
        RWLock l(&rwlock_, false);
        return bgsave_path_;
    }

    //TODO: 17/3/5 by zmyer
    std::string bgsave_prefix() {
        RWLock l(&rwlock_, false);
        return bgsave_prefix_;
    }

    //TODO: 17/3/5 by zmyer
    std::string userpass() {
        RWLock l(&rwlock_, false);
        return userpass_;
    }

    //TODO: 17/3/5 by zmyer
    const std::string suser_blacklist() {
        RWLock l(&rwlock_, false);
        return slash::StringConcat(user_blacklist_, COMMA);
    }

    //TODO: 17/3/5 by zmyer
    const std::vector<std::string> &vuser_blacklist() {
        RWLock l(&rwlock_, false);
        return user_blacklist_;
    }

    // TODO: 17/3/2 by zmyer
    std::string compression() {
        RWLock l(&rwlock_, false);
        return compression_;
    }

    //TODO: 17/3/5 by zmyer
    int target_file_size_base() {
        RWLock l(&rwlock_, false);
        return target_file_size_base_;
    }

    //TODO: 17/3/5 by zmyer
    int max_background_flushes() {
        RWLock l(&rwlock_, false);
        return max_background_flushes_;
    }

    //TODO: 17/3/5 by zmyer
    int max_background_compactions() {
        RWLock l(&rwlock_, false);
        return max_background_compactions_;
    }

    //TODO: 17/3/5 by zmyer
    int max_cache_files() {
        RWLock l(&rwlock_, false);
        //最大的缓冲文件数目
        return max_cache_files_;
    }

    //TODO: 17/3/5 by zmyer
    int expire_logs_nums() {
        RWLock l(&rwlock_, false);
        return expire_logs_nums_;
    }

    //TODO: 17/3/5 by zmyer
    int expire_logs_days() {
        RWLock l(&rwlock_, false);
        return expire_logs_days_;
    }

    //TODO: 17/3/5 by zmyer
    std::string conf_path() {
        RWLock l(&rwlock_, false);
        return conf_path_;
    }

    //TODO: 17/3/5 by zmyer
    bool readonly() {
        RWLock l(&rwlock_, false);
        return readonly_;
    }

    // TODO: 17/3/2 by zmyer
    int maxclients() {
        RWLock l(&rwlock_, false);
        return maxclients_;
    }

    //TODO: 17/3/5 by zmyer
    int root_connection_num() {
        RWLock l(&rwlock_, false);
        return root_connection_num_;
    }

    //TODO: 17/3/5 by zmyer
    int slowlog_slower_than() {
        RWLock l(&rwlock_, false);
        return slowlog_log_slower_than_;
    }

    //TODO: 17/3/5 by zmyer
    std::string network_interface() {
        RWLock l(&rwlock_, false);
        return network_interface_;
    }

    // Immutable config items, we don't use lock.
    // TODO: 17/3/2 by zmyer
    bool daemonize() { return daemonize_; }

    //TODO: 17/3/5 by zmyer
    std::string pidfile() { return pidfile_; }

    //TODO: 17/3/5 by zmyer
    int binlog_file_size() { return binlog_file_size_; }

    // Setter
    //TODO: 17/3/5 by zmyer
    void SetPort(const int value) {
        RWLock l(&rwlock_, true);
        port_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetThreadNum(const int value) {
        RWLock l(&rwlock_, true);
        thread_num_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetLogLevel(const int value) {
        RWLock l(&rwlock_, true);
        log_level_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetTimeout(const int value) {
        RWLock l(&rwlock_, true);
        timeout_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetSlaveof(const std::string value) {
        RWLock l(&rwlock_, true);
        slaveof_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetBgsavePath(const std::string &value) {
        RWLock l(&rwlock_, true);
        bgsave_path_ = value;
        if (value[value.length() - 1] != '/') {
            bgsave_path_ += "/";
        }
    }

    //TODO: 17/3/5 by zmyer
    void SetBgsavePrefix(const std::string &value) {
        RWLock l(&rwlock_, true);
        bgsave_prefix_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetRequirePass(const std::string &value) {
        RWLock l(&rwlock_, true);
        requirepass_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetMasterAuth(const std::string &value) {
        RWLock l(&rwlock_, true);
        masterauth_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetSlotMigrate(const std::string &value) {
        RWLock l(&rwlock_, true);
        slotmigrate_ = (value == "yes") ? true : false;
    }

    //TODO: 17/3/5 by zmyer
    void SetUserPass(const std::string &value) {
        RWLock l(&rwlock_, true);
        userpass_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetUserBlackList(const std::string &value) {
        RWLock l(&rwlock_, true);
        slash::StringSplit(value, COMMA, user_blacklist_);
    }

    //TODO: 17/3/5 by zmyer
    void SetReadonly(const bool value) {
        RWLock l(&rwlock_, true);
        readonly_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetExpireLogsNums(const int value) {
        RWLock l(&rwlock_, true);
        expire_logs_nums_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetExpireLogsDays(const int value) {
        RWLock l(&rwlock_, true);
        expire_logs_days_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetMaxConnection(const int value) {
        RWLock l(&rwlock_, true);
        maxclients_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetRootConnectionNum(const int value) {
        RWLock l(&rwlock_, true);
        root_connection_num_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetSlowlogSlowerThan(const int value) {
        RWLock l(&rwlock_, true);
        slowlog_log_slower_than_ = value;
    }

    //TODO: 17/3/5 by zmyer
    void SetDbSyncSpeed(const int value) {
        RWLock l(&rwlock_, true);
        db_sync_speed_ = value;
    }

    int Load();

    int ConfigRewrite();

private:
    //端口号
    int port_;
    //slave节点信息
    std::string slaveof_;
    //线程数目
    int thread_num_;
    //同步线程数目
    int sync_thread_num_;
    //同步缓冲区的长度
    int sync_buffer_size_;
    //日志路径
    std::string log_path_;
    //数据库的路径
    std::string db_path_;
    //数据同步路径
    std::string db_sync_path_;
    //数据库同步速率
    int db_sync_speed_;
    //写入缓冲区的长度
    int write_buffer_size_;
    //日志等级
    int log_level_;
    //守护进程标记
    bool daemonize_;
    //
    bool slotmigrate_;
    //超时时间
    int timeout_;
    std::string requirepass_;
    std::string masterauth_;
    std::string userpass_;
    std::vector<std::string> user_blacklist_;
    std::string bgsave_path_;
    std::string bgsave_prefix_;
    std::string pidfile_;

    //char pidfile_[PIKA_WORD_SIZE];
    std::string compression_;
    int maxclients_;
    int root_connection_num_;
    int slowlog_log_slower_than_;
    int expire_logs_days_;
    int expire_logs_nums_;
    bool readonly_;
    std::string conf_path_;
    int max_background_flushes_;
    int max_background_compactions_;
    int max_cache_files_;
    std::string network_interface_;

    //char username_[30];
    //char password_[30];

    //
    // Critical configure items
    //
    int target_file_size_base_;
    int binlog_file_size_;

    pthread_rwlock_t rwlock_;
};

#endif
