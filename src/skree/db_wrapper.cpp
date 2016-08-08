#include "db_wrapper.hpp"

namespace Skree {
    bool DbWrapper::add(
        const char * kbuf,
        size_t ksiz,
        const char * vbuf,
        size_t vsiz
    ) {
        // Utils::cluck(2, "db.add(%s)", kbuf);
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
        // fprintf(stderr, "db_wrapper::remove(");
        // for(size_t i = 0; i < ksiz; ++i) {
        //     fprintf(stderr, "%.2X", kbuf[i]);
        // }
        // Utils::cluck(1, ")");
        // abort();
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
}
