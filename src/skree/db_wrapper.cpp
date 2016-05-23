#include "db_wrapper.hpp"

namespace Skree {
    bool DbWrapper::add(
        const char * kbuf,
        size_t ksiz,
        const char * vbuf,
        size_t vsiz
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::add(kbuf, ksiz, vbuf, vsiz);
        unlock();
        return rv;
    }

    bool DbWrapper::set(
        const char * kbuf,
        size_t ksiz,
        const char * vbuf,
        size_t vsiz
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::set(kbuf, ksiz, vbuf, vsiz);
        unlock();
        return rv;
    }

    bool DbWrapper::remove(
        const char * kbuf,
        size_t ksiz
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::remove(kbuf, ksiz);
        unlock();
        return rv;
    }

    int64_t DbWrapper::increment(
        const char * kbuf,
        size_t ksiz,
        int64_t num,
        int64_t orig
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::increment(kbuf, ksiz, num, orig);
        unlock();
        return rv;
    }

    int64_t DbWrapper::remove_bulk(
        const std::vector<std::string> & keys,
        bool atomic
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::remove_bulk(keys, atomic);
        unlock();
        return rv;
    }

    bool DbWrapper::cas(
        const char * kbuf,
        size_t ksiz,
        const char * ovbuf,
        size_t ovsiz,
        const char * nvbuf,
        size_t nvsiz
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz);
        unlock();
        return rv;
    }

    bool DbWrapper::synchronize(
        bool hard,
        kyotocabinet::BasicDB::FileProcessor * proc,
        kyotocabinet::BasicDB::ProgressChecker * checker
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::synchronize(hard, proc, checker);
        unlock();
        return rv;
    }

    bool DbWrapper::begin_transaction(bool hard) {
        lock();
        auto rv = kyotocabinet::HashDB::begin_transaction(hard);
        // unlock();
        return rv;
    }

    bool DbWrapper::end_transaction(bool commit) {
        // lock();
        auto rv = kyotocabinet::HashDB::end_transaction(commit);
        unlock();
        return rv;
    }

    char* DbWrapper::get(
        const char * kbuf,
        size_t ksiz,
        size_t * sp
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::get(kbuf, ksiz, sp);
        unlock();
        return rv;
    }

    bool DbWrapper::accept_bulk(
        const std::vector< std::string > & keys,
        kyotocabinet::DB::Visitor * visitor,
        bool writable
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::accept_bulk(keys, visitor, writable);
        unlock();
        return rv;
    }

    int32_t DbWrapper::check(
        const char * kbuf,
        size_t ksiz
    ) {
        lock();
        auto rv = kyotocabinet::HashDB::check(kbuf, ksiz);
        unlock();
        return rv;
    }

    get_keys_result_t* DbWrapper::db_get_keys(std::vector<std::string>& keys) {
        class VisitorImpl : public kyotocabinet::DB::Visitor {
            public:
                explicit VisitorImpl(get_keys_result_t* _out) : out(_out) {}

            private:
                const char* visit_full(
                    const char* _key, size_t key_len,
                    const char* _value, size_t value_len,
                    size_t* sp
                ) {
                    char* key = (char*)malloc(key_len + 1);
                    memcpy(key, _key, key_len);
                    key[key_len] = '\0';
                    // if(strncmp(key,"wrinseq",7)!=0)
                    // printf("got %s\n",key);

                    char* value = (char*)malloc(value_len);
                    memcpy(value, _value, value_len);

                    (*out)[key] = (Utils::muh_str_t*)malloc(sizeof(Utils::muh_str_t));
                    (*out)[key]->len = value_len;
                    (*out)[key]->data = value;

                    return kyotocabinet::DB::Visitor::NOP;
                }

                get_keys_result_t* out;
        };

        get_keys_result_t* out = new get_keys_result_t();

        VisitorImpl visitor(out);

        if(accept_bulk(keys, &visitor, false)) return out;
        else return NULL;
    }
}
