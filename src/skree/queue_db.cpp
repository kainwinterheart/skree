#include "queue_db.hpp"
// #include <execinfo.h>

namespace Skree {
    QueueDb::QueueDb(const char* _path, size_t _file_size) : path(_path), file_size(_file_size) {
        path_len = strlen(path);

        uint64_t page_num, page_offset;

        get_page_num("rpos", page_num, page_offset);
        read_page = new QueueDb::Page(
            path_len, path, file_size,
            page_num, page_offset, /*async=*/false,
            /*close_fhs=*/true, async_allocators, O_RDONLY
        );

        get_page_num("wpos", page_num, page_offset);
        write_page = new QueueDb::Page(
            path_len, path, file_size,
            page_num, page_offset, /*async=*/false,
            /*close_fhs=*/true, async_allocators, O_RDWR
        );

        kv = new DbWrapper;

        std::string db_file_name (path, path_len);
        db_file_name.append("/skree.kch");

        if(!kv->open(
            db_file_name,
            kyotocabinet::HashDB::OWRITER
            | kyotocabinet::HashDB::OCREATE
            | kyotocabinet::HashDB::ONOLOCK
            | kyotocabinet::HashDB::OAUTOTRAN
        )) {
            fprintf(stderr, "Failed to open database: %s\n", kv->error().name());
            abort();
        }

        read_page->get_next();
        write_page->get_next();
    }

    QueueDb::~QueueDb() {
        delete read_page;
        delete write_page;
        delete kv;
    }

    QueueDb::Page::Page(
        size_t& _path_len, const char*& _path, size_t& _file_size,
        uint64_t _num, uint64_t _offset, bool _async, bool _close_fhs,
        QueueDb::async_allocators_t& _async_allocators, int _flag
    )
        : path_len(_path_len)
        , path(_path)
        , recommended_file_size(_file_size)
        , num(_num)
        , offset(_offset)
        , async(_async)
        , close_fhs(_close_fhs)
        , async_allocators(_async_allocators)
        , flag(_flag)
    {
        pthread_mutex_init(&mutex, nullptr);

        fh = -1;
        addr = nullptr;
        next = nullptr;
        effective_file_size = 0;
        open_page();

        if(!async) get_next();
    }

    QueueDb::Page::~Page() {
        pthread_mutex_destroy(&mutex);

        if(close_fhs) {
            if(addr != nullptr) {
                munmap(addr, effective_file_size);
            }

            if(fh != -1) {
                close(fh);
            }
        }
    }

    void QueueDb::get_page_num(const char* name, uint64_t& page_num, uint64_t& offset) const {
        auto name_len = strlen(name);
        char file [path_len + 1 + name_len + 1];

        memcpy(file, path, path_len);
        file[path_len] = '/';
        memcpy(file + path_len + 1, name, name_len);
        file[path_len + 1 + name_len] = '\0';

        int fh = open(file, O_RDWR | O_CREAT);

        if(fh == -1) {
            perror("open");
            abort();

        } else {
            fchmod(fh, 0000644);
        }

        read_uint64(fh, page_num);
        read_uint64(fh, offset);

        if(close(fh) == -1) {
            perror("close");
        }
    }

    void QueueDb::read_uint64(int& fh, std::atomic<uint64_t>& dest) const {
        uint64_t value;
        read_uint64(fh, value);
        dest = value;
    }

    void QueueDb::read_uint64(int& fh, uint64_t& dest) const {
        int read_total = 0;
        int read;

        while(
            ((read = ::read(fh, (&dest + read_total), (8 - read_total))) > 0)
            && (read_total < 8)
        ) {
            read_total += read;
        }

        dest = ((read_total == 8) ? ntohll(dest) : 0);
    }

    size_t QueueDb::Page::alloc_page(const char* file) const {
        int fh = open(file, O_RDWR | O_CREAT);

        if(fh == -1) {
            perror("open");
            abort();
        }

        fchmod(fh, 0000644);

        size_t total = 0;
        char* batch = (char*)malloc(SKREE_QUEUEDB_ZERO_BATCH_SIZE);
        memset(batch, 0, SKREE_QUEUEDB_ZERO_BATCH_SIZE);
        size_t chunk_size;
        size_t written;

        while(total < recommended_file_size) {
            chunk_size = (
                ((recommended_file_size - total) > SKREE_QUEUEDB_ZERO_BATCH_SIZE)
                    ? SKREE_QUEUEDB_ZERO_BATCH_SIZE
                    : (recommended_file_size - total)
            );
            written = Utils::write_chunk(fh, chunk_size, batch);
            total += written;

            if(chunk_size != written) {
                break;
            }
        }

        free(batch);

        if(close(fh) == -1) {
            perror("close");
        }

        return total;
    }

    void QueueDb::Page::async_alloc_page(char* _file) {
        auto end = async_allocators.lock();
        auto it = async_allocators.find(num);

        if(it != end) {
            async_allocators.unlock();
            return;
        }

        async_allocators[num] = new QueueDb::AsyncAllocatorsItem {
            .sz = 0
        };
        async_allocators[num]->done = 0;
        async_allocators.unlock();

        char* file = strdup(_file);

        auto cb = [this, file](){
            auto size = alloc_page(file);
            auto end = async_allocators.lock();
            auto it = async_allocators.find(num);
            async_allocators.unlock();

            if(it == end) {
                abort();

            } else {
                it->second->sz = size;
                it->second->done = 1;
                free(file);
            }
        };

        auto thread = new Skree::Workers::QueueDbAsyncAlloc<decltype(cb)>(cb);
        thread->start();
    }

    void QueueDb::Page::open_page(bool force_sync) {
        if(fh != -1) return;

        if(async) {
            auto end = async_allocators.lock();
            auto it = async_allocators.find(num);
            async_allocators.unlock();

            if(it != end) {
                if(force_sync) {
                    while(it->second->done == 0) {
                        usleep(500);
                    }

                    effective_file_size = it->second->sz;
                    delete it->second;

                    async_allocators.lock();
                    async_allocators.erase(it);
                    async_allocators.unlock();

                } else {
                    return;
                }
            }
        }

        char file [path_len + 1 + 21 + 1];
        int access_flag = 0;
        int mmap_prot = 0;
        bool known_flag = false;

        memcpy(file, path, path_len);
        file[path_len] = '/';
        sprintf(file + path_len + 1, "%lu", num.load());

        if((flag & O_RDONLY) || (flag == O_RDONLY) || (flag & O_RDWR) || (flag == O_RDWR)) {
            access_flag |= R_OK;
            mmap_prot |= PROT_READ;
            known_flag = true;
        }

        if((flag & O_WRONLY) || (flag == O_WRONLY) || (flag & O_RDWR) || (flag == O_RDWR)) {
            access_flag |= W_OK;
            mmap_prot |= PROT_WRITE;
            known_flag = true;
        }

        if(!known_flag) {
            // fprintf(stderr, "flag: %lld, access_flag: %lld, mmap_prot: %lld, O_RDONLY: %lld, O_WRONLY: %lld, O_RDWR: %lld\n", flag, access_flag, mmap_prot, O_RDONLY, O_WRONLY, O_RDWR);
            throw new std::logic_error ("QueueDb::open_page: Bad flags");
        }

        if(effective_file_size == 0) {
            if(access(file, access_flag) == -1) {
                if(async && !force_sync) {
                    async_alloc_page(file);

                } else {
                    effective_file_size = alloc_page(file);
                }

            } else {
                struct stat fh_stat;

                if(stat(file, &fh_stat) == -1) {
                    perror("fstat");

                } else {
                    effective_file_size = fh_stat.st_size;

                    if(effective_file_size == 0) {
                        if(unlink(file) == 0) {
                            if(async && !force_sync) {
                                async_alloc_page(file);

                            } else {
                                effective_file_size = alloc_page(file);
                            }

                        } else {
                            perror("unlink");
                        }
                    }
                }
            }
        }

        if(async && !force_sync) return;

        if(effective_file_size == 0) {
            fprintf(stderr, "Zero-length file: %s\n", file);
            abort();
        }

        fh = open(file, flag);

        if(fh == -1) {
            perror(file);
            abort();
        }

        addr = (char*)mmap(0, effective_file_size, mmap_prot, MAP_FILE | MAP_SHARED, fh, 0); // TODO: restore MAP_NOCACHE

        if(addr == MAP_FAILED) {
            perror("mmap");
            abort();
        }
    }

    QueueDb::Page* QueueDb::Page::get_next() {
        if(next != nullptr) return next;

        next = new QueueDb::Page(
            path_len, path, recommended_file_size,
            num + 1, /*offset=*/0, /*async=*/true,
            /*close_fhs=*/false, async_allocators, flag
        );

        // {
        //     void* callstack[128];
        //     int frames = backtrace(callstack, 128);
        //     char** strs = backtrace_symbols(callstack, frames);
        //
        //     for(int i = 0; i < frames; ++i) {
        //         printf("get_next(%d) [%d]: %s\n", num.load(), i, strs[i]);
        //     }
        //     printf("\n");
        //
        //     free(strs);
        // }

        return next;
    }

    void QueueDb::atomic_sync_offset(const char* name, uint64_t& page_num, uint64_t& page_offset) const {
        auto name_len = strlen(name);
        auto file_len = path_len + 1 + name_len;
        auto tmp_file_len = file_len + 4;

        char file [file_len + 1];
        char tmp_file [tmp_file_len + 1];

        memcpy(file, path, path_len);
        file[path_len] = '/';
        memcpy(file + path_len + 1, name, name_len);
        file[path_len + 1 + name_len] = '\0';

        memcpy(tmp_file, file, file_len);
        tmp_file[file_len] = '.';
        tmp_file[file_len + 1] = 'n';
        tmp_file[file_len + 2] = 'e';
        tmp_file[file_len + 3] = 'w';
        tmp_file[tmp_file_len] = '\0';

        int fh = open(tmp_file, O_RDWR | O_CREAT);

        if(fh == -1) {
            perror("open");
            abort();

        } else {
            fchmod(fh, 0000644);
        }

        // TODO: check result
        bool written = (
            (Utils::write_chunk(fh, sizeof(page_num), &page_num) == sizeof(page_num))
            && (Utils::write_chunk(fh, sizeof(page_offset), &page_offset) == sizeof(page_offset))
        );

        if(close(fh) == -1) {
            perror("close");
        }

        if(written) {
            if(rename(tmp_file, file) == -1) {
                perror("rename");
                abort();
            }

        } else {
            abort();
        }
    }

    void QueueDb::sync_read_offset(bool commit) {
        if(read_rollbacks.empty()) {
            throw new std::logic_error ("There are no read rollbacks, what are you syncing?");
        }

        if(commit) {
            uint64_t _read_page_num = read_page->get_num();
            uint64_t _read_page_offset = read_page->get_offset();
            auto page = read_page;

            while(_read_page_offset == read_page->get_effective_file_size()) {
                page = page->get_next();
                _read_page_num = page->get_num();
                _read_page_offset = page->get_offset();
            }

            _read_page_num = htonll(_read_page_num);
            _read_page_offset = htonll(_read_page_offset);

            atomic_sync_offset("rpos", _read_page_num, _read_page_offset);
            read_page->close_if_can();

            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                read_page->_free_read_rollback(_rollback);
            }

        } else {
            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                read_page->_rollback_read(_rollback);
            }
        }
    }

    void QueueDb::sync_write_offset() {
        uint64_t _write_page_num = write_page->get_num();
        uint64_t _write_page_offset = write_page->get_offset();
        auto page = write_page;

        while(_write_page_offset == write_page->get_effective_file_size()) {
            page = page->get_next();
            _write_page_num = page->get_num();
            _write_page_offset = page->get_offset();
        }

        _write_page_num = htonll(_write_page_num);
        _write_page_offset = htonll(_write_page_offset);

        atomic_sync_offset("wpos", _write_page_num, _write_page_offset);
        write_page->close_if_can();
    }

    void QueueDb::Page::close_if_can() {
        if(next == nullptr) return;

        next->close_if_can();

        if(
            (addr != nullptr)
            && (next->addr != nullptr)
            && (offset == effective_file_size)
            && (addr != next->addr)
        ) {
            // fprintf(stderr, "read_page_file_size(%d): %d\n", read_page_num, read_page_file_size);
            munmap(addr, effective_file_size);
            close(fh);

            addr = next->addr;
            fh = next->fh;
            effective_file_size = next->effective_file_size.load();
            num = next->num.load();
            offset = next->offset.load();

            auto prev_next = next;
            next = next->get_next();
            delete prev_next;
        }
    }

    QueueDb::read_rollback_t* QueueDb::_read(uint64_t len, unsigned char* dest) {
        return read_page->_read(len, dest);
    }

    QueueDb::read_rollback_t* QueueDb::Page::_read(uint64_t len, unsigned char* dest) {
        uint64_t rest = (effective_file_size - offset);
        // fprintf(stderr, "read_page_num: %llu, need to read: %llu, free: %llu, addr: 0x%llx, read_page_offset: %llu\n", read_page_num, len, rest, this, read_page_offset);
        auto rollback = new QueueDb::read_rollback_t {
            .l1 = 0,
            .l2 = nullptr
        };

        if(rest >= len) {
            memcpy(dest, addr + offset, len);
            offset += len;
            rollback->l1 = len;

            // for(int i = 0; i < len; ++i)
            //     printf("read byte [%d]: 0x%.2X\n", i,dest[i]);

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(dest, addr + offset, rest);
                offset += rest;
                rollback->l1 = rest;
            }

            auto next = get_next();

            if(next->async) next->open_page(true);

            rollback->l2 = next->_read(len, dest + rest);
        }

        return rollback;
    }

    void QueueDb::Page::_rollback_read(QueueDb::read_rollback_t* rollback) {
        if(rollback->l1 > 0) {
            offset -= rollback->l1;
        }

        if(rollback->l2 != nullptr) {
            next->_rollback_read(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::Page::_free_read_rollback(QueueDb::read_rollback_t* rollback) const {
        if(rollback->l2 != nullptr) {
            _free_read_rollback(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::_write(uint64_t len, const unsigned char* src) {
        write_page->_write(len, src);
    }

    void QueueDb::Page::_write(uint64_t len, const unsigned char* src) {
        uint64_t rest = (effective_file_size - offset);
        // fprintf(stderr, "write_page_num: %llu, need to write: %llu, free: %llu, addr: 0x%llx, write_page_offset: %llu\n", num.load(), len, rest, this, offset.load());
        if(rest >= len) {
            // printf("write_page: 0x%lx, write_page_offset: %lu, src: 0x%lx\n", (intptr_t)write_page, write_page_offset, (intptr_t)src);
            memcpy(addr + offset, src, len);
            offset += len;

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(addr + offset, src, rest);
                offset += rest;
            }

            // fprintf(stderr, "before get_next\n");
            auto next = get_next();
            // fprintf(stderr, "after get_next\n");

            if(next->async) next->open_page(true);
            // fprintf(stderr, "after open_page\n");

            next->_write(len, src + rest);
            // fprintf(stderr, "after write\n");
        }
    }

    char* QueueDb::read(uint64_t* _len) {
        if(!read_rollbacks.empty()) {
            throw new std::logic_error ("You haven't committed previous read, why are you trying to read more?");
        }

        write_page->lock();

        if(
            (read_page->get_num() == write_page->get_num())
            && (read_page->get_offset() >= write_page->get_offset())
        ) {
            write_page->unlock();
            // TODO: read rollbacks may not be needed after this
            return nullptr;
        }

        write_page->unlock();
        read_page->open_page(true);

        char* out = nullptr;
        uint64_t len;

        // pthread_mutex_lock(&read_page_mutex);

        auto rollback1 = read_page->_read(sizeof(len), (unsigned char*)&len);
        len = ntohll(len);

        if(len == 0) {
            // fprintf(stderr, "QueueDb: got zero-length item, rollback\n");
            read_page->_rollback_read(rollback1);
            throw new std::logic_error ("QueueDb: got zero-length item, rollback");

        } else {
            out = (char*)malloc(len);
            auto rollback2 = read_page->_read(len, (unsigned char*)out);
            // _free_read_rollback(rollback2);
            // _free_read_rollback(rollback1);
            read_rollbacks.push(rollback1);
            read_rollbacks.push(rollback2);
        }

        if(_len != nullptr) {
            *_len = len;
        }

        // pthread_mutex_unlock(&read_page_mutex);

        return out;
    }

    QueueDb::WriteStream* QueueDb::write() {
        return new QueueDb::WriteStream (*this);
    }

    void QueueDb::write(uint64_t len, void* data) {
        if(len == 0) {
            throw new std::logic_error("QueueDb: zero-length write, ignoring it");
        }

        uint64_t _len = htonll(len);

        write_page->lock();

        write_page->_write(sizeof(_len), (const unsigned char*)&_len);
        write_page->_write(len, (const unsigned char*)data);
        sync_write_offset();

        write_page->unlock();
    }

    void QueueDb::WriteStream::write(uint64_t len, void* data) {
        db._write(len, (const unsigned char*)data);
        total_len += len;
    }

    QueueDb::WriteStream::WriteStream(QueueDb& _db) : db(_db) {
        db.write_page->lock();
        db.write_page->open_page(true);

        last_page = db.write_page;

        while(last_page->get_effective_file_size() == last_page->get_offset()) {
            last_page = last_page->get_next();

            if(last_page->get_async()) last_page->open_page(true);
        }

        begin_offset = last_page->get_offset();
        total_len = 0;

        const uint64_t _total_len (htonll(total_len));
        db._write(sizeof(_total_len), (const unsigned char*)&_total_len);
    }

    QueueDb::WriteStream::~WriteStream() {
        uint64_t end_offset = last_page->get_offset();

        last_page->set_offset(begin_offset);

        bool wrap = ((last_page->get_effective_file_size() - begin_offset) < sizeof(total_len));
        uint64_t next_end_offset;

        if(wrap) {
            auto next = last_page->get_next();
            next_end_offset = next->get_offset();
            next->set_offset(0);
        }

        if(total_len > 0) {
            const uint64_t _total_len (htonll(total_len));
            db._write(sizeof(_total_len), (const unsigned char*)&_total_len);

            last_page->set_offset(end_offset);

            if(wrap) {
                auto next = last_page->get_next();
                next->set_offset(next_end_offset);
            }

            db.sync_write_offset();
            db.write_page->unlock();

        } else {
            db.write_page->unlock();
            throw new std::logic_error("QueueDb: zero-length write, ignoring it");
        }
    }
}
