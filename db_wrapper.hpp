#ifndef _DB_WRAPPER_H_
#define _DB_WRAPPER_H_

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wconversion"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wfloat-equal"
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wswitch-enum"
#pragma clang diagnostic ignored "-Warray-bounds-pointer-arithmetic"
#pragma clang diagnostic ignored "-Wweak-vtables"
#include <kchashdb.h>
#pragma clang diagnostic pop

#include <pthread.h>
#include <vector>
#include <string>

class DbWrapper : public kyotocabinet::HashDB {

public:
    DbWrapper() : kyotocabinet::HashDB() {

        pthread_mutexattr_init( &mutexattr );
        pthread_mutexattr_settype( &mutexattr, PTHREAD_MUTEX_RECURSIVE );
        pthread_mutex_init( &mutex, &mutexattr );
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
        kyotocabinet::BasicDB::FileProcessor * proc = NULL,
        kyotocabinet::BasicDB::ProgressChecker * checker = NULL
    );

    virtual bool begin_transaction(bool hard = false);
    virtual bool end_transaction(bool commit = true);

    virtual char* get(
        const char * kbuf,
        size_t ksiz,
        size_t * sp
    );

    virtual bool accept_bulk(
        const std::vector< std::string > & keys,
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

#endif
