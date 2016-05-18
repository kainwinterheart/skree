#ifndef _SKREE_DBWRAPPER_H_
#define _SKREE_DBWRAPPER_H_

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

        get_keys_result_t* db_get_keys(std::vector<std::string>& keys);

        template <typename T>
        T* parse_db_value(Utils::muh_str_t* _value, size_t* size = nullptr) {
            if(_value == nullptr) return nullptr;
            if(size != nullptr) *size = _value->len;

            T* value = (T*)malloc(_value->len);
            memcpy(value, _value->data, _value->len);

            // free(_value->data);
            delete _value;

            return value;
        }

        template <typename T>
        T* parse_db_value(get_keys_result_t* map, std::string* key, size_t* size = nullptr) {
            get_keys_result_t::iterator it = map->find((char*)(key->c_str()));

            if(it == map->end()) return nullptr;

            return parse_db_value<T>(it->second, size);
        }

    private:
        pthread_mutex_t mutex;
        pthread_mutexattr_t mutexattr;

        void lock() { } //{ pthread_mutex_lock(&mutex); }
        void unlock() { } //{ pthread_mutex_unlock(&mutex); }
    };
}

#endif
