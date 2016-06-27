#pragma once
#define SKREE_QUEUEDB_ZERO_BATCH_SIZE (4 * 1024 * 1024)

#include "utils/misc.hpp"
#include "db_wrapper.hpp"
#include "workers/queue_db_async_alloc.hpp"
#include "utils/atomic_hash_map.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <stack>
#include <sys/stat.h>

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
        char* read_page;
        int read_page_fh;
        pthread_mutex_t read_page_mutex;
        size_t read_page_file_size;

        uint64_t write_page_num;
        uint64_t write_page_offset;
        char* write_page;
        int write_page_fh;
        pthread_mutex_t write_page_mutex;
        size_t write_page_file_size;

        std::stack<read_rollback_t*> read_rollbacks;

        struct AsyncAllocatorsItem {
            size_t sz;
            std::atomic<uint_fast16_t> done;
        };

        Skree::Utils::AtomicHashMap<uint64_t, QueueDb::AsyncAllocatorsItem*> async_allocators;
        bool async;

        void get_page_num(const char* name, uint64_t& page_num, uint64_t& offset) const;
        void atomic_sync_offset(const char* file, uint64_t& page_num, uint64_t& page_offset) const;
        void read_uint64(int& fh, uint64_t& dest) const;
        void open_page(
            int& fh, int flag, const uint64_t& num, char*& addr,
            size_t& page_file_size, bool force_sync = false
        );
        size_t alloc_page(const char* file) const;
        bool write_chunk(int fh, size_t size, void* data) const;
        void async_alloc_page(char* file, uint64_t num);

        void open_read_page(bool force_sync = false) {
            open_page(read_page_fh, O_RDONLY, read_page_num, read_page, read_page_file_size, force_sync);
        }

        void open_write_page(bool force_sync = false) {
            open_page(write_page_fh, O_RDWR, write_page_num, write_page, write_page_file_size, force_sync);
        }

        inline void _write(uint64_t len, const unsigned char* src);
        inline read_rollback_t* _read(uint64_t len, unsigned char* dest);
        inline void _rollback_read(read_rollback_t* rollback);
        inline void _free_read_rollback(read_rollback_t* rollback) const;

        bool close_fhs;
        void close_if_can();
        void sync_write_offset();
        QueueDb* get_next_page();

        explicit QueueDb(
            const char* _path, size_t _file_size, uint64_t _read_page_num,
            uint64_t _write_page_num, DbWrapper* _kv
        );
    public:
        DbWrapper* kv;

        explicit QueueDb(const char* _path, size_t _file_size);
        ~QueueDb();

        char* read(uint64_t* len = nullptr);
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
        void write(uint64_t len, void* data);
    };
}
