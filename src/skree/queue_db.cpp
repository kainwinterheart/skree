#include "queue_db.hpp"

namespace Skree {
    QueueDb::QueueDb(const char* _path, size_t _file_size) : path(_path), file_size(_file_size) {
        async = false;
        close_fhs = true;
        path_len = strlen(path);
        next_page = nullptr;
        pthread_mutex_init(&read_page_mutex, nullptr);
        pthread_mutex_init(&write_page_mutex, nullptr);

        read_page = nullptr;
        read_page_fh = -1;
        read_page_file_size = 0;
        get_page_num("rpos", read_page_num, read_page_offset);
        open_read_page();

        write_page = nullptr;
        write_page_fh = -1;
        write_page_file_size = 0;
        get_page_num("wpos", write_page_num, write_page_offset);
        open_write_page();

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
            exit(1);
        }

        get_next_page();
    }

    QueueDb::QueueDb(
        const char* _path, size_t _file_size, uint64_t _read_page_num,
        uint64_t _write_page_num, DbWrapper* _kv
    )
    : path(_path),
      file_size(_file_size),
      kv(_kv),
      read_page_num(_read_page_num),
      write_page_num(_write_page_num) {
        async = true;
        close_fhs = false;
        path_len = strlen(path);
        next_page = nullptr;
        pthread_mutex_init(&read_page_mutex, nullptr);
        pthread_mutex_init(&write_page_mutex, nullptr);

        read_page = nullptr;
        read_page_fh = -1;
        read_page_offset = 0;
        read_page_file_size = 0;
        open_read_page();

        write_page = nullptr;
        write_page_fh = -1;
        write_page_offset = 0;
        write_page_file_size = 0;
        open_write_page();
    }

    QueueDb::~QueueDb() {
        pthread_mutex_destroy(&write_page_mutex);
        pthread_mutex_destroy(&read_page_mutex);

        if(close_fhs) {
            close(read_page_fh);
            close(write_page_fh);
        }

        delete kv;
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
            exit(1);

        } else {
            fchmod(fh, 0000644);
        }

        read_uint64(fh, page_num);
        read_uint64(fh, offset);

        if(close(fh) == -1) {
            perror("close");
        }
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

    bool QueueDb::write_chunk(int fh, size_t size, void* data) const {
        size_t written;
        size_t total = 0;

        while(total < size) {
            written = ::write(fh, ((unsigned char*)data) + total, size - total);

            if(written == -1) {
                perror("write");
                break;

            } else {
                total += written;
            }
        }

        return (total == size);
    }

    size_t QueueDb::alloc_page(const char* file) const {
        int fh = open(file, O_RDWR | O_CREAT);

        if(fh == -1) {
            perror("open");
            exit(1);
        }

        fchmod(fh, 0000644);

        size_t total = 0;
        char* batch = (char*)malloc(SKREE_QUEUEDB_ZERO_BATCH_SIZE);
        memset(batch, 0, SKREE_QUEUEDB_ZERO_BATCH_SIZE);
        size_t chunk_size;

        while(total < file_size) {
            chunk_size = (
                ((file_size - total) > SKREE_QUEUEDB_ZERO_BATCH_SIZE)
                    ? SKREE_QUEUEDB_ZERO_BATCH_SIZE
                    : (file_size - total)
            );

            if(write_chunk(fh, chunk_size, batch)) {
                total += chunk_size;

            } else {
                break;
            }
        }

        free(batch);

        if(close(fh) == -1) {
            perror("close");
        }

        return total;
    }

    void QueueDb::async_alloc_page(char* _file, uint64_t num) {
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

        auto cb = [this, file, num](){
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

    void QueueDb::open_page(
        int& fh, int flag, const uint64_t& num, char*& addr,
        size_t& page_file_size, bool force_sync
    ) {
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

                    page_file_size = it->second->sz;
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
        sprintf(file + path_len + 1, "%lu", num);

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

        if(page_file_size == 0) {
            if(access(file, access_flag) == -1) {
                if(async && !force_sync) {
                    async_alloc_page(file, num);

                } else {
                    page_file_size = alloc_page(file);
                }

            } else {
                struct stat fh_stat;

                if(stat(file, &fh_stat) == -1) {
                    perror("fstat");

                } else {
                    page_file_size = fh_stat.st_size;

                    if(page_file_size == 0) {
                        if(unlink(file) == 0) {
                            if(async && !force_sync) {
                                async_alloc_page(file, num);

                            } else {
                                page_file_size = alloc_page(file);
                            }

                        } else {
                            perror("unlink");
                        }
                    }
                }
            }
        }

        if(async && !force_sync) return;

        if(page_file_size == 0) {
            fprintf(stderr, "Zero-length file: %s\n", file);
            abort();
        }

        fh = open(file, flag);

        if(fh == -1) {
            perror("open");
            exit(1);
        }

        addr = (char*)mmap(0, page_file_size, mmap_prot, MAP_FILE | MAP_SHARED, fh, 0); // TODO: restore MAP_NOCACHE

        if(addr == MAP_FAILED) {
            perror("mmap");
            exit(1);
        }
    }

    QueueDb* QueueDb::get_next_page() {
        if(next_page != nullptr) return next_page;

        next_page = new QueueDb(
            path, file_size, read_page_num + 1,
            write_page_num + 1, kv
        );

        return next_page;
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
            exit(1);

        } else {
            fchmod(fh, 0000644);
        }

        // TODO: check result
        bool written = (
            write_chunk(fh, sizeof(page_num), &page_num)
            && write_chunk(fh, sizeof(page_offset), &page_offset)
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
            uint64_t _read_page_num = read_page_num;
            uint64_t _read_page_offset = read_page_offset;
            auto db = this;

            while(_read_page_offset == read_page_file_size) {
                db = db->next_page;
                _read_page_num = db->read_page_num;
                _read_page_offset = db->read_page_offset;
            }

            _read_page_num = htonll(_read_page_num);
            _read_page_offset = htonll(_read_page_offset);

            atomic_sync_offset("rpos", _read_page_num, _read_page_offset);

            pthread_mutex_lock(&write_page_mutex);
            close_if_can();
            pthread_mutex_unlock(&write_page_mutex);

            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                _free_read_rollback(_rollback);
            }

        } else {
            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                _rollback_read(_rollback);
            }
        }
    }

    void QueueDb::sync_write_offset() {
        uint64_t _write_page_num = write_page_num;
        uint64_t _write_page_offset = write_page_offset;
        auto db = this;

        while(_write_page_offset == write_page_file_size) {
            db = db->next_page;
            _write_page_num = db->write_page_num;
            _write_page_offset = db->write_page_offset;
        }

        _write_page_num = htonll(_write_page_num);
        _write_page_offset = htonll(_write_page_offset);

        atomic_sync_offset("wpos", _write_page_num, _write_page_offset);
        close_if_can();
    }

    void QueueDb::close_if_can() {
        if(next_page == nullptr) return;

        next_page->close_if_can();

        if(
            (read_page_offset == read_page_file_size)
            && (read_page != next_page->read_page)
        ) {
            munmap(read_page, read_page_file_size);
            close(read_page_fh);

            read_page = next_page->read_page;
            read_page_fh = next_page->read_page_fh;
            read_page_file_size = next_page->read_page_file_size;
            next_page->get_next_page();
        }

        if(
            (write_page_offset == write_page_file_size)
            && (write_page != next_page->write_page)
        ) {
            munmap(write_page, write_page_file_size);
            close(write_page_fh);

            write_page = next_page->write_page;
            write_page_fh = next_page->write_page_fh;
            write_page_file_size = next_page->write_page_file_size;
            next_page->get_next_page();
        }

        while(
            (next_page != nullptr)
            && (read_page == next_page->read_page)
            && (write_page == next_page->write_page)
        ) {
            read_page_num = next_page->read_page_num;
            write_page_num = next_page->write_page_num;

            read_page_offset = next_page->read_page_offset;
            write_page_offset = next_page->write_page_offset;

            auto prev_next_page = next_page;
            next_page = next_page->next_page;

            delete prev_next_page;
        }
    }

    read_rollback_t* QueueDb::_read(uint64_t len, unsigned char* dest) {
        uint64_t rest = (read_page_file_size - read_page_offset);
        auto rollback = new read_rollback_t {
            .l1 = 0,
            .l2 = nullptr
        };

        if(rest >= len) {
            memcpy(dest, read_page + read_page_offset, len);
            read_page_offset += len;
            rollback->l1 = len;

            // for(int i = 0; i < len; ++i)
            //     printf("read byte [%d]: 0x%.2X\n", i,dest[i]);

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(dest, read_page + read_page_offset, rest);
                read_page_offset += rest;
                rollback->l1 = rest;
            }

            auto next = get_next_page();
            rollback->l2 = next->_read(len, dest + rest);
        }

        return rollback;
    }

    void QueueDb::_rollback_read(read_rollback_t* rollback) {
        if(rollback->l1 > 0) {
            read_page_offset -= rollback->l1;
        }

        if(rollback->l2 != nullptr) {
            auto next = get_next_page();
            next->_rollback_read(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::_free_read_rollback(read_rollback_t* rollback) const {
        if(rollback->l2 != nullptr) {
            _free_read_rollback(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::_write(uint64_t len, const unsigned char* src) {
        uint64_t rest = (write_page_file_size - write_page_offset);

        if(rest >= len) {
            // printf("write_page: 0x%lx, write_page_offset: %lu, src: 0x%lx\n", (intptr_t)write_page, write_page_offset, (intptr_t)src);
            memcpy(write_page + write_page_offset, src, len);
            write_page_offset += len;

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(write_page + write_page_offset, src, rest);
                write_page_offset += rest;
            }

            auto next = get_next_page();
            next->_write(len, src + rest);
        }
    }

    char* QueueDb::read(uint64_t* _len) {
        if(!read_rollbacks.empty()) {
            throw new std::logic_error ("You haven't committed previous read, why are you trying to read more?");
        }

        pthread_mutex_lock(&write_page_mutex);

        if(
            (read_page_num == write_page_num)
            && (read_page_offset >= write_page_offset)
        ) {
            pthread_mutex_unlock(&write_page_mutex);
            // TODO: read rollbacks may not be needed after this
            return nullptr;
        }

        pthread_mutex_unlock(&write_page_mutex);

        if(async) open_read_page(true);

        char* out = nullptr;
        uint64_t len;

        // pthread_mutex_lock(&read_page_mutex);

        auto rollback1 = _read(sizeof(len), (unsigned char*)&len);
        len = ntohll(len);

        if(len == 0) {
            // fprintf(stderr, "QueueDb: got zero-length item, rollback\n");
            _rollback_read(rollback1);
            throw new std::logic_error ("QueueDb: got zero-length item, rollback");

        } else {
            out = (char*)malloc(len);
            auto rollback2 = _read(len, (unsigned char*)out);
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
        if(async) {
            pthread_mutex_lock(&write_page_mutex);
            open_write_page(true);
            pthread_mutex_unlock(&write_page_mutex);
        }

        return new QueueDb::WriteStream (*this);
    }

    void QueueDb::write(uint64_t len, void* data) {
        if(len == 0) {
            throw new std::logic_error("QueueDb: zero-length write, ignoring it");
        }

        uint64_t _len = htonll(len);

        pthread_mutex_lock(&write_page_mutex);

        if(async) open_write_page(true);

        _write(sizeof(_len), (const unsigned char*)&_len);
        _write(len, (const unsigned char*)data);
        sync_write_offset();

        pthread_mutex_unlock(&write_page_mutex);
    }

    void QueueDb::WriteStream::write(uint64_t len, void* data) {
        db._write(len, (const unsigned char*)data);
        total_len += len;
    }

    QueueDb::WriteStream::WriteStream(QueueDb& _db) : db(_db) {
        pthread_mutex_lock(&(db.write_page_mutex));

        last_page = &db;

        while(last_page->write_page_file_size == last_page->write_page_offset) {
            last_page = last_page->get_next_page();
        }

        begin_offset = last_page->write_page_offset;
        total_len = 0;

        const uint64_t _total_len (htonll(total_len));
        db._write(sizeof(_total_len), (const unsigned char*)&_total_len);
    }

    QueueDb::WriteStream::~WriteStream() {
        uint64_t end_offset = last_page->write_page_offset;

        last_page->write_page_offset = begin_offset;

        bool wrap = (
            (last_page->write_page_file_size - last_page->write_page_offset)
            < sizeof(total_len)
        );
        uint64_t next_end_offset;

        if(wrap) {
            auto& next = last_page->next_page;
            next_end_offset = next->write_page_offset;
            next->write_page_offset = 0;
        }

        if(total_len > 0) {
            const uint64_t _total_len (htonll(total_len));
            db._write(sizeof(_total_len), (const unsigned char*)&_total_len);

            last_page->write_page_offset = end_offset;

            if(wrap) {
                auto& next = last_page->next_page;
                next->write_page_offset = next_end_offset;
            }

            db.sync_write_offset();
            pthread_mutex_unlock(&(db.write_page_mutex));

        } else {
            pthread_mutex_unlock(&(db.write_page_mutex));
            throw new std::logic_error("QueueDb: zero-length write, ignoring it");
        }
    }
}
