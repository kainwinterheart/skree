#pragma once

#include <kchashdb.h>

#include "utils/misc.hpp"
// #include <pthread.h>
// #include <vector>
// #include <string>

namespace Skree {
    typedef std::unordered_map<char*, Utils::muh_str_t*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> get_keys_result_t;

    class DbWrapper : public kyotocabinet::HashDB {
    public:
        DbWrapper() : kyotocabinet::HashDB() {
            pthread_mutexattr_init(&mutexattr);
            pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE);
            pthread_mutex_init(&mutex,&mutexattr);
        }

        virtual bool add(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        );

        virtual bool set(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        );

        virtual bool remove(
            const char * kbuf,
            size_t ksiz
        );

        virtual int64_t increment(
            const char * kbuf,
            size_t ksiz,
            int64_t num,
            int64_t orig = 0
        );

        virtual int64_t remove_bulk(
            const std::vector< std::string > & keys,
            bool atomic = true
        );

        virtual bool cas(
            const char * kbuf,
            size_t ksiz,
            const char * ovbuf,
            size_t ovsiz,
            const char * nvbuf,
            size_t nvsiz
        );

        virtual bool synchronize(
            bool hard = false,
            kyotocabinet::BasicDB::FileProcessor * proc = nullptr,
            kyotocabinet::BasicDB::ProgressChecker * checker = nullptr
        );

        virtual bool begin_transaction(bool hard = false);
        virtual bool end_transaction(bool commit = true);

        virtual char* get(
            const char * kbuf,
            size_t ksiz,
            size_t * sp
        );

        virtual bool accept_bulk(
            const std::vector<std::string> & keys,
            kyotocabinet::DB::Visitor * visitor,
            bool writable = true
        );

        virtual int32_t check(
            const char * kbuf,
            size_t ksiz
        );

    private:
        pthread_mutex_t mutex;
        pthread_mutexattr_t mutexattr;

        void lock() { pthread_mutex_lock(&mutex); }
        void unlock() { pthread_mutex_unlock(&mutex); }
    };
}
