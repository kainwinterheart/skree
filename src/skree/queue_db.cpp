#include "queue_db.hpp"
// #include <execinfo.h>

namespace Skree {
    QueueDb::QueueDb(const char* _path, size_t _file_size) : path(_path), file_size(_file_size) {
        path_len = strlen(path);

        {
            QueueDb::Page::Args args {
                .path_len = path_len,
                .path = path,
                .recommended_file_size = file_size,
                .pos_file = get_page_num_file("rpos"),
                .async_allocators = async_allocators,
                .flag = O_RDONLY
            };

            read_page = new QueueDb::Page(args);
        }

        {
            QueueDb::Page::Args args {
                .path_len = path_len,
                .path = path,
                .recommended_file_size = file_size,
                .pos_file = get_page_num_file("wpos"),
                .async_allocators = async_allocators,
                .flag = O_RDWR
            };

            write_page = new QueueDb::Page(args);
        }

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
            Utils::cluck(2, "Failed to open database: %s\n", kv->error().name());
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

    QueueDb::Page::Page(Args& _args)
        : args(_args)
        , async(false)
        , close_fhs(true)
        , fh(-1)
        , addr(nullptr)
        , next(nullptr)
        , effective_file_size(0)
        , loaded(false)
    {
        pthread_mutex_init(&mutex, nullptr);

        auto* addr = args.pos_file->begin();

        block = *(uint8_t*)addr;
        num = ntohll(*(uint64_t*)(addr + 1 + (block * (8 * 2))));
        offset = ntohll(*(uint64_t*)(addr + 1 + (block * (8 * 2)) + 8));

        open_page();
        get_next();
    }

    QueueDb::Page::Page(Page* prev)
        : args(prev->args)
        , num(prev->num + 1)
        , offset(0)
        , async(true)
        , close_fhs(false)
        , block(prev->block.load()) // TODO?
        , fh(-1)
        , addr(nullptr)
        , next(nullptr)
        , effective_file_size(0)
        , loaded(false)
    {
        pthread_mutex_init(&mutex, nullptr);

        open_page();
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

    Utils::MappedFile* QueueDb::get_page_num_file(const char* name) const {
        auto name_len = strlen(name);
        char file [path_len + 1 + name_len + 1];

        memcpy(file, path, path_len);
        file[path_len] = '/';
        memcpy(file + path_len + 1, name, name_len);
        file[path_len + 1 + name_len] = '\0';

        return new Utils::MappedFile(file, 1 + 8 * 2 * 2);
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

        while(total < args.recommended_file_size) {
            chunk_size = (
                ((args.recommended_file_size - total) > SKREE_QUEUEDB_ZERO_BATCH_SIZE)
                    ? SKREE_QUEUEDB_ZERO_BATCH_SIZE
                    : (args.recommended_file_size - total)
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
        auto end = args.async_allocators.lock();
        auto it = args.async_allocators.find(num);

        if(it != end) {
            args.async_allocators.unlock();
            return;
        }

        args.async_allocators[num].reset(new QueueDb::AsyncAllocatorsItem {
            .sz = 0
        });
        args.async_allocators[num]->done = 0;

        char* file = strdup(_file);

        auto cb = [this, file](){
            auto size = alloc_page(file);
            auto end = args.async_allocators.lock();
            auto it = args.async_allocators.find(num);
            args.async_allocators.unlock();

            if(it == end) {
                abort();

            } else {
                it->second->sz = size;
                it->second->done = 1;
                free(file);
            }
        };

        auto thread = std::make_shared<Skree::Workers::QueueDbAsyncAlloc<decltype(cb)>>(cb);
        thread->start();

        args.async_allocators[num]->thread = std::shared_ptr<void>(thread, (void*)thread.get());
        args.async_allocators.unlock();
    }

    void QueueDb::Page::open_page(bool force_sync) {
        if(fh != -1) return;

        if(async) {
            auto end = args.async_allocators.lock();
            auto it = args.async_allocators.find(num);
            args.async_allocators.unlock();

            if(it != end) {
                if(force_sync) {
                    while(it->second->done == 0) {
                        usleep(500); // TODO?
                    }

                    effective_file_size = it->second->sz;
                    // delete it->second;

                    args.async_allocators.lock();
                    args.async_allocators.erase(it);
                    args.async_allocators.unlock();

                } else {
                    return;
                }
            }
        }

        char file [args.path_len + 1 + 21 + 1];
        int access_flag = 0;
        int mmap_prot = 0;
        bool known_flag = false;

        memcpy(file, args.path, args.path_len);
        file[args.path_len] = '/';
        sprintf(file + args.path_len + 1, "%lu", num.load());

        auto flag = args.flag;

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
            throw std::logic_error ("QueueDb::open_page: Bad flags");
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
            Utils::cluck(2, "Zero-length file: %s\n", file);
            abort();
        }

        fh = open(file, flag);

        if(fh == -1) {
            perror(file);
            abort();
        }

        addr = (char*)mmap(0, effective_file_size, mmap_prot, MAP_FILE | MAP_SHARED, fh, 0); // TODO: restore MAP_NOCACHE
        loaded = true;

        if(addr == MAP_FAILED) {
            perror("mmap");
            abort();
        }
    }

    QueueDb::Page* QueueDb::Page::get_next() {
        if(next != nullptr) return next;
        return next = new QueueDb::Page(this);
    }

    void QueueDb::Page::atomic_sync_offset(const uint64_t& _offset, const uint64_t& _page_num) {
        uint8_t new_block = (1 - block);
        auto* addr = args.pos_file->begin();

        *(uint64_t*)(addr + 1 + (new_block * (8 * 2)) + 8) = htonll(_offset);
        *(uint64_t*)(addr + 1 + (new_block * (8 * 2))) = htonll(_page_num);
        block = *(uint8_t*)addr = new_block;

        // args.pos_file->sync();
    }

    void QueueDb::sync_read_offset(bool commit) {
        if(read_rollbacks.empty()) {
            throw std::logic_error ("There are no read rollbacks, what are you syncing?");
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

            read_page->atomic_sync_offset(_read_page_offset, _read_page_num);
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

        write_page->atomic_sync_offset(_write_page_offset, _write_page_num);
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
            // Utils::cluck(3, "read_page_file_size(%d): %d\n", read_page_num, read_page_file_size);
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
        // Utils::cluck(6, "read_page_num: %llu, need to read: %llu, free: %llu, addr: 0x%llx, read_page_offset: %llu\n", read_page_num, len, rest, this, read_page_offset);
        auto rollback = new QueueDb::read_rollback_t {
            .l1 = 0,
            .l2 = nullptr
        };

        if(rest >= len) {
            memcpy(dest, addr + offset, len);
            offset += len;
            rollback->l1 = len;

            // for(int i = 0; i < len; ++i)
            //     Utils::cluck(3, "read byte [%d]: 0x%.2X\n", i,dest[i]);

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
        // Utils::cluck(6, "write_page_num: %llu, need to write: %llu, free: %llu, addr: 0x%llx, write_page_offset: %llu\n", num.load(), len, rest, this, offset.load());
        if(rest >= len) {
            // Utils::cluck(4, "write_page: 0x%lx, write_page_offset: %lu, src: 0x%lx\n", (intptr_t)write_page, write_page_offset, (intptr_t)src);
            memcpy(addr + offset, src, len);
            offset += len;

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(addr + offset, src, rest);
                offset += rest;
            }

            // Utils::cluck(1, "before get_next\n");
            auto next = get_next();
            // Utils::cluck(1, "after get_next\n");

            if(next->async) next->open_page(true);
            // Utils::cluck(1, "after open_page\n");

            next->_write(len, src + rest);
            // Utils::cluck(1, "after write\n");
        }
    }

    std::shared_ptr<Utils::muh_str_t> QueueDb::read() {
        if(!read_rollbacks.empty()) {
            throw std::logic_error ("You haven't committed previous read, why are you trying to read more?");
        }

        write_page->lock();

        if(
            (read_page->get_num() == write_page->get_num())
            && (read_page->get_offset() >= write_page->get_offset())
        ) {
            write_page->unlock();
            // TODO: read rollbacks may not be needed after this
            return std::shared_ptr<Utils::muh_str_t>();
        }

        write_page->unlock();
        read_page->open_page(true);

        std::shared_ptr<Utils::muh_str_t> out;
        uint64_t len; // TODO: uint32_t

        // pthread_mutex_lock(&read_page_mutex);

        auto rollback1 = read_page->_read(sizeof(len), (unsigned char*)&len);
        len = ntohll(len);

        if(len == 0) {
            // Utils::cluck(1, "QueueDb: got zero-length item, rollback\n");
            read_page->_rollback_read(rollback1);
            throw std::logic_error ("QueueDb: got zero-length item, rollback");

        } else {
            out.reset(new Utils::muh_str_t {
                .own = true,
                .len = (uint32_t)len, // TODO
                .data = (char*)malloc(len)
            });

            auto rollback2 = read_page->_read(len, (unsigned char*)out->data);
            // _free_read_rollback(rollback2);
            // _free_read_rollback(rollback1);
            read_rollbacks.push(rollback1);
            read_rollbacks.push(rollback2);
        }

        // pthread_mutex_unlock(&read_page_mutex);

        return out;
    }

    QueueDb::WriteStream* QueueDb::write() {
        return new QueueDb::WriteStream (*this);
    }

    void QueueDb::write(uint64_t len, const void* data) {
        if(len == 0) {
            throw std::logic_error("QueueDb: zero-length write, ignoring it");
        }

        uint64_t _len = htonll(len);

        write_page->lock();

        write_page->_write(sizeof(_len), (const unsigned char*)&_len);
        write_page->_write(len, (const unsigned char*)data);
        sync_write_offset();

        write_page->unlock();
    }

    void QueueDb::WriteStream::write(uint64_t len, const void* data) {
        db._write(len, (const unsigned char*)data);
        total_len += len;
    }

    void QueueDb::WriteStream::write(const Skree::Utils::StringSequence& sequence) {
        for(const auto& item : sequence) {
            write(item.len, item.data);
        }
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

        bool wrap = ((last_page->get_effective_file_size() - begin_offset) < total_len);
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
            throw std::logic_error("QueueDb: zero-length write, ignoring it");
        }
    }
}
