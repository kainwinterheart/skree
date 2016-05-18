#ifndef _SKREE_QUEUEDB_H_
#define _SKREE_QUEUEDB_H_

#define SKREE_QUEUEDB_ZERO_BATCH_SIZE 4096

#include "utils/misc.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <stack>

namespace Skree {
    struct read_rollback_t {
        uint64_t l1;
        read_rollback_t* l2;
    };

    class QueueDb {
    private:
        QueueDb* next_page;
        const char* path;
        size_t path_len;
        size_t file_size;

        uint64_t read_page_num;
        uint64_t read_page_offset;
        int read_page_num_fh;
        char* read_page;
        int read_page_fh;
        pthread_mutex_t read_page_mutex;

        uint64_t write_page_num;
        uint64_t write_page_offset;
        int write_page_num_fh;
        char* write_page;
        int write_page_fh;
        pthread_mutex_t write_page_mutex;

        std::stack<read_rollback_t*> read_rollbacks;

        void get_page_num(
            const char* name, int& fh,
            uint64_t& page_num, uint64_t& offset
        ) const;

        void read_uint64(int& fh, uint64_t& dest) const;
        void open_page(int& fh, int flag, const uint64_t& num, char*& addr) const;

        void open_read_page() {
            open_page(read_page_fh, O_RDONLY, read_page_num, read_page);
        }

        void open_write_page() {
            open_page(write_page_fh, O_RDWR, write_page_num, write_page);
        }

        inline void _write(uint64_t len, const unsigned char* src);
        inline read_rollback_t* _read(uint64_t len, unsigned char* dest);
        inline void _rollback_read(read_rollback_t* rollback);
        inline void _free_read_rollback(read_rollback_t* rollback) const;

        bool close_fhs;
        void close_if_can();
        void sync_write_offset();
        QueueDb* get_next_page();

        QueueDb(
            const char* _path, size_t _file_size, uint64_t _read_page_num,
            uint64_t _write_page_num, int _read_page_num_fh, int _write_page_num_fh
        ) : path(_path), file_size(_file_size), read_page_num(_read_page_num), write_page_num(_write_page_num), read_page_num_fh(_read_page_num_fh), write_page_num_fh(_write_page_num_fh) {
            close_fhs = false;
            path_len = strlen(path);
            next_page = nullptr;
            pthread_mutex_init(&read_page_mutex, nullptr);
            pthread_mutex_init(&write_page_mutex, nullptr);

            read_page = nullptr;
            read_page_fh = -1;
            read_page_offset = 0;
            open_read_page();

            write_page = nullptr;
            write_page_fh = -1;
            write_page_offset = 0;
            open_write_page();
        }
    public:
        QueueDb(const char* _path, size_t _file_size) : path(_path), file_size(_file_size) {
            close_fhs = true;
            path_len = strlen(path);
            next_page = nullptr;
            pthread_mutex_init(&read_page_mutex, nullptr);
            pthread_mutex_init(&write_page_mutex, nullptr);

            read_page = nullptr;
            read_page_fh = -1;
            read_page_num_fh = -1;
            get_page_num("rpos", read_page_num_fh, read_page_num, read_page_offset);
            open_read_page();

            write_page = nullptr;
            write_page_fh = -1;
            write_page_num_fh = -1;
            get_page_num("wpos", write_page_num_fh, write_page_num, write_page_offset);
            open_write_page();
        }

        ~QueueDb() {
            pthread_mutex_destroy(&write_page_mutex);
            pthread_mutex_destroy(&read_page_mutex);

            if(close_fhs) {
                close(read_page_num_fh);
                close(write_page_num_fh);
                close(read_page_fh);
                close(write_page_fh);
            }
        }

        char* read();
        void sync_read_offset(bool commit = true);

        class WriteStream {
        private:
            uint64_t begin_offset;
            uint64_t total_len;
            QueueDb& db;
            QueueDb* last_page;
        public:
            WriteStream(QueueDb& _db);
            ~WriteStream();
            void write(uint64_t len, void* data);
        };

        friend class QueueDb::WriteStream;

        QueueDb::WriteStream* write();
    };
}

#endif
