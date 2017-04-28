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
        const char* path;
        size_t path_len;
        size_t file_size;
        std::shared_ptr<DbWrapper::TReader> Reader;

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

        std::shared_ptr<Utils::muh_str_t> read(uint64_t& key) {
            std::shared_ptr<Utils::muh_str_t> out;
            DbWrapper::TReader::TData value;

            if(Reader->Read(&key, value)) {
                out.reset(new Utils::muh_str_t(
                    (char*)malloc((uint32_t)value.size), (uint32_t)value.size, true
                ));

                memcpy(out->data, value.data, out->len);
            }

            return out;
        }

        inline void sync() {
            kv->NewSession(DbWrapper::TSession::ST_KV)->Sync();
            kv->NewSession(DbWrapper::TSession::ST_QUEUE)->Sync();
        }

        bool sync_read_offset(bool commit) {
            (void)commit;
            // return /*(commit ? true : */Reader->StepBack()/*)*/;
            // return (commit ? true : Reader->Reset());
            return Reader->Reset();
            // return true;
        }
        bool Reset() {
            return Reader->Reset();
            // return true;
        }
        bool Reset2() {
            // return Reader->Reset();
            return true;
        }
    };
}
