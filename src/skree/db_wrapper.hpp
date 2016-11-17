#pragma once

#include "utils/misc.hpp"
#include "utils/mapped_file.hpp"
#include <rocksdb/db.h>
// #include <pthread.h>
// #include <vector>
// #include <string>

namespace Skree {
    // typedef std::unordered_map<char*, Utils::muh_str_t*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> get_keys_result_t;

    class DbWrapper {
    private:
        std::shared_ptr<rocksdb::DB> Db;
        std::shared_ptr<Utils::MappedFile> PkFile;

    public:
        DbWrapper(std::string&& dbFileName);

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

        virtual uint64_t increment(uint64_t num);

        virtual bool cas(
            const char * kbuf,
            size_t ksiz,
            const char * ovbuf,
            size_t ovsiz,
            const char * nvbuf,
            size_t nvsiz
        );

        virtual bool synchronize(bool hard = false);

        virtual bool begin_transaction(bool hard = false);
        virtual bool end_transaction(bool commit = true);

        virtual std::string get(
            const char * kbuf,
            size_t ksiz
        );

        virtual int32_t check(
            const char * kbuf,
            size_t ksiz
        );
    };
}
