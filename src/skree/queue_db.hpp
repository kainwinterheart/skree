#pragma once
#define SKREE_QUEUEDB_ZERO_BATCH_SIZE (4 * 1024 * 1024)

#include "utils/misc.hpp"
#include "db_wrapper.hpp"
#include "workers/queue_db_async_alloc.hpp"
#include "utils/atomic_hash_map.hpp"
#include "utils/string_sequence.hpp"
#include "utils/mapped_file.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <stack>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

namespace Skree {
    class QueueDb {
    private:
        struct AsyncAllocatorsItem {
            size_t sz;
            std::atomic<uint_fast16_t> done;
        };

        struct read_rollback_t {
            uint64_t l1;
            QueueDb::read_rollback_t* l2;
        };

        typedef Skree::Utils::AtomicHashMap<uint64_t, QueueDb::AsyncAllocatorsItem*> async_allocators_t;

        class Page {
        public:
            struct Args {
                size_t& recommended_file_size;
                const char* path;
                size_t path_len;
                QueueDb::async_allocators_t& async_allocators;
                int flag;
                Utils::MappedFile* pos_file;
            };

        private:
            char* addr;
            int fh;
            pthread_mutex_t mutex;
            std::atomic<size_t> effective_file_size;
            Page* next;
            Args args;

            bool async;
            bool close_fhs;
            std::atomic<uint64_t> num;
            std::atomic<uint64_t> offset;
            std::atomic<uint8_t> block; // TODO?
            std::atomic<bool> loaded;

            size_t alloc_page(const char* file) const;
            void async_alloc_page(char* _file);

            inline void sync_data() const {
                if(!loaded) return;

                if(msync(addr, effective_file_size, MS_SYNC) == -1) {
                    char _addr [21];
                    sprintf(_addr, "0x%lx", (uintptr_t)addr);

                    std::string str ("msync(");
                    str += _addr;
                    str += ')';

                    perror(str.c_str());
                    abort();
                }

                if(fsync(fh) == -1) {
                    perror("fsync");
                    abort();
                }
            }

        public:
            Page* get_next();

            explicit Page(Args& _args);
            explicit Page(Page* prev);
            virtual ~Page();

            void open_page(bool force_sync = false);
            void close_if_can();
            inline void _rollback_read(QueueDb::read_rollback_t* rollback);
            inline void _free_read_rollback(QueueDb::read_rollback_t* rollback) const;
            inline void _write(uint64_t len, const unsigned char* src);
            inline QueueDb::read_rollback_t* _read(uint64_t len, unsigned char* dest);

            inline void lock() {
                pthread_mutex_lock(&mutex);
            }

            inline void unlock() {
                pthread_mutex_unlock(&mutex);
            }

            inline uint64_t get_num() const {
                return num;
            }

            inline uint64_t get_offset() const {
                return offset;
            }

            inline void set_offset(uint64_t _offset) {
                offset = _offset;
            }

            inline bool get_async() const {
                return async;
            }

            inline size_t get_effective_file_size() const {
                return effective_file_size;
            }

            inline void sync_all_data() {
                Page* node = this;

                while(node != nullptr) {
                    node->sync_data();
                    node = node->next;
                }
            }

            inline void sync_pos() {
                args.pos_file->sync();
            }

            inline void atomic_sync_offset(const uint64_t& page_num, const uint64_t& page_offset);
        };

        friend class QueueDb::Page;

        const char* path;
        size_t path_len;
        size_t file_size;

        QueueDb::Page* read_page;
        QueueDb::Page* write_page;

        std::stack<QueueDb::read_rollback_t*> read_rollbacks;
        async_allocators_t async_allocators;

        Utils::MappedFile* get_page_num_file(const char* name) const;
        inline void _write(uint64_t len, const unsigned char* src);
        inline QueueDb::read_rollback_t* _read(uint64_t len, unsigned char* dest);
        void sync_write_offset();
    public:
        DbWrapper* kv;

        inline const char* get_path() const {
            return path;
        }

        inline const size_t get_path_len() const {
            return path_len;
        }

        explicit QueueDb(const char* _path, size_t _file_size);
        ~QueueDb();

        char* read(uint64_t* len = nullptr);
        void sync_read_offset(bool commit = true);

        inline uint64_t get_first_used_page_num() const {
            return std::min(read_page->get_num(), write_page->get_num());
        }

        class WriteStream {
        private:
            uint64_t begin_offset;
            uint64_t total_len;
            QueueDb& db;
            QueueDb::Page* last_page;
        public:
            WriteStream(QueueDb& _db);
            ~WriteStream();
            void write(uint64_t len, const void* data);
            void write(const Skree::Utils::StringSequence& sequence);
        };

        friend class QueueDb::WriteStream;

        QueueDb::WriteStream* write();
        void write(uint64_t len, const void* data);

        inline void sync() {
            write_page->sync_all_data();
            write_page->sync_pos();
            read_page->sync_pos();
        }
    };
}
